import os
import re
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from functions import schedule_tools
from functions.logger import logger


DEFAULT_BASE_URL = "https://www.istu.edu/schedule/"
DEFAULT_TIMEOUT_SEC = 20
DEFAULT_RETRIES = 2
DEFAULT_MAX_WORKERS = 8
DEFAULT_MIN_SUCCESS_RATE = 0.7


def _normalize_spaces(value: str) -> str:
    return " ".join(value.split()) if value else ""


def _parse_query_int(href: str, key: str) -> Optional[int]:
    match = re.search(r"[?&]" + re.escape(key) + r"=(\d+)", href or "")
    if not match:
        return None
    return int(match.group(1))


def _unique_preserve_order(items: List[str]) -> List[str]:
    result = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _normalize_course_name(raw_course: str) -> str:
    match = re.search(r"(\d+)", raw_course or "")
    if match:
        return f"{match.group(1)} курс"
    return _normalize_spaces(raw_course) if raw_course else "1 курс"


def _normalize_week(classes: List[str]) -> Optional[str]:
    classes_set = set(classes or [])
    if "class-all-week" in classes_set:
        return "all"
    if "class-even-week" in classes_set:
        return "even"
    if "class-odd-week" in classes_set:
        return "odd"
    return None


def _build_info(lesson_type: str, subgroup: Optional[str]) -> str:
    lesson_type_lower = (lesson_type or "").lower()
    if "лекц" in lesson_type_lower:
        normalized_type = "Лекция"
    elif "практ" in lesson_type_lower:
        normalized_type = "Практ."
    elif "лаб" in lesson_type_lower:
        normalized_type = "Лаб. раб."
    elif lesson_type:
        normalized_type = lesson_type
    else:
        normalized_type = "Занятие"

    if subgroup:
        return f"( {normalized_type} подгруппа {subgroup} )"
    return f"( {normalized_type} )"


def parse_subdivisions_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    subdivisions = []
    seen_subdiv_ids = set()

    for anchor in soup.select('a[href*="subdiv="]'):
        href = anchor.get("href", "")
        subdiv_id = _parse_query_int(href, "subdiv")
        if subdiv_id is None or subdiv_id in seen_subdiv_ids:
            continue

        name = _normalize_spaces(anchor.get_text(" ", strip=True))
        if not name:
            continue

        seen_subdiv_ids.add(subdiv_id)
        subdivisions.append({
            "subdiv_id": subdiv_id,
            "institute": name,
        })

    return subdivisions


def parse_groups_html(html: str, institute: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    result = []
    seen_group_ids = set()

    kurs_list = soup.find("ul", class_="kurs-list")
    if kurs_list:
        for course_li in kurs_list.find_all("li", recursive=False):
            top_level_text = []
            for node in course_li.contents:
                if isinstance(node, NavigableString):
                    top_level_text.append(str(node))
            course_name = _normalize_course_name(_normalize_spaces(" ".join(top_level_text)))

            groups_ul = course_li.find("ul")
            if not groups_ul:
                continue

            for anchor in groups_ul.find_all("a", href=True):
                group_id = _parse_query_int(anchor["href"], "group")
                if group_id is None or group_id in seen_group_ids:
                    continue

                group_name = _normalize_spaces(anchor.get_text(" ", strip=True))
                if not group_name:
                    continue

                seen_group_ids.add(group_id)
                result.append({
                    "group_id": group_id,
                    "name": group_name,
                    "course": course_name,
                    "institute": institute,
                })

    if result:
        return result

    # Fallback: parse all group links when course wrappers are absent.
    for anchor in soup.select('a[href*="group="]'):
        href = anchor.get("href", "")
        group_id = _parse_query_int(href, "group")
        if group_id is None or group_id in seen_group_ids:
            continue

        group_name = _normalize_spaces(anchor.get_text(" ", strip=True))
        if not group_name:
            continue

        seen_group_ids.add(group_id)
        result.append({
            "group_id": group_id,
            "name": group_name,
            "course": "1 курс",
            "institute": institute,
        })

    return result


def _extract_group_name(soup: BeautifulSoup, fallback_group_name: str) -> str:
    for paragraph in soup.select("div.alert-info p"):
        text = _normalize_spaces(paragraph.get_text(" ", strip=True)).lower()
        if "группа:" not in text:
            continue

        bold = paragraph.find("b")
        if bold:
            group_name = _normalize_spaces(bold.get_text(" ", strip=True))
            if group_name:
                return group_name

    return fallback_group_name


def _extract_lesson_type_and_preps(first_info: Tag) -> Tuple[str, List[str]]:
    lesson_type_parts = []
    preps = []

    for node in first_info.contents:
        if isinstance(node, NavigableString):
            lesson_type_parts.append(str(node))

    for prep_link in first_info.find_all("a", href=True):
        prep_name = _normalize_spaces(prep_link.get_text(" ", strip=True))
        if not prep_name:
            continue
        prep_id = _parse_query_int(prep_link["href"], "prep")
        preps.append((prep_id, prep_name))

    lesson_type = _normalize_spaces(" ".join(lesson_type_parts))
    return lesson_type, _unique_preserve_order(
        [f"{prep_id}:{prep_name}" for prep_id, prep_name in preps]
    )


def _decode_preps(encoded_preps: List[str]) -> List[Tuple[Optional[int], str]]:
    result = []
    for item in encoded_preps:
        if ":" not in item:
            continue
        prep_id_str, prep_name = item.split(":", 1)
        prep_id = int(prep_id_str) if prep_id_str.isdigit() else None
        result.append((prep_id, prep_name))
    return result


def _sort_day_lessons(lessons: List[Dict[str, Any]]) -> None:
    lessons.sort(key=lambda item: item["info"])
    lessons.sort(key=lambda item: int(item["time"].replace(":", "")))


def _merge_group_lesson(day_lessons: List[Dict[str, Any]], lesson: Dict[str, Any]) -> None:
    for day_lesson in day_lessons:
        same_signature = (
            day_lesson["time"] == lesson["time"]
            and day_lesson["week"] == lesson["week"]
            and day_lesson["name"] == lesson["name"]
            and day_lesson["info"] == lesson["info"]
        )
        if not same_signature:
            continue

        if day_lesson["aud"] == lesson["aud"]:
            day_lesson["prep"] = _unique_preserve_order(day_lesson["prep"] + lesson["prep"])
            return
        if day_lesson["prep"] == lesson["prep"]:
            day_lesson["aud"] = _unique_preserve_order(day_lesson["aud"] + lesson["aud"])
            return
    day_lessons.append(lesson)


def parse_group_schedule_html(
    html: str,
    fallback_group_name: str,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    soup = BeautifulSoup(html, "html.parser")
    group_name = _extract_group_name(soup, fallback_group_name)

    schedule_container = soup.find("div", class_="full-odd-week")
    if not schedule_container:
        return group_name, [], []

    day_to_lessons: Dict[str, List[Dict[str, Any]]] = {}
    events: List[Dict[str, Any]] = []
    valid_days = set(schedule_tools.DAYS.values())

    for day_heading in schedule_container.find_all("h3", class_="day-heading"):
        day_heading_text = _normalize_spaces(day_heading.get_text(" ", strip=True))
        if "," not in day_heading_text:
            continue

        day_name = day_heading_text.split(",", 1)[0].strip().lower()
        if day_name not in valid_days:
            continue

        class_lines = day_heading.find_next_sibling("div", class_="class-lines")
        if not class_lines:
            continue

        if day_name not in day_to_lessons:
            day_to_lessons[day_name] = []

        for class_line in class_lines.find_all("div", class_="class-line-item", recursive=False):
            class_tails = class_line.find("div", class_="class-tails")
            if not class_tails:
                continue

            class_time_tag = class_tails.find("div", class_="class-time")
            if not class_time_tag:
                continue
            class_time = _normalize_spaces(class_time_tag.get_text(" ", strip=True))
            if not class_time:
                continue

            for tail in class_tails.find_all("div", class_="class-tail", recursive=False):
                week = _normalize_week(tail.get("class", []))
                if not week:
                    continue

                tail_text = _normalize_spaces(tail.get_text(" ", strip=True)).lower()
                if "свободно" in tail_text:
                    _merge_group_lesson(
                        day_to_lessons[day_name],
                        {
                            "time": class_time,
                            "week": week,
                            "name": "свободно",
                            "aud": [""],
                            "info": "",
                            "prep": [""],
                        },
                    )
                    continue

                class_pred = tail.find("div", class_="class-pred")
                lesson_name = _normalize_spaces(class_pred.get_text(" ", strip=True)) if class_pred else ""
                if not lesson_name:
                    continue

                info_blocks = tail.find_all("div", class_="class-info", recursive=False)
                first_info = info_blocks[0] if info_blocks else None
                second_info = info_blocks[1] if len(info_blocks) > 1 else None

                lesson_type = ""
                prep_meta: List[Tuple[Optional[int], str]] = []
                if first_info:
                    lesson_type, encoded_preps = _extract_lesson_type_and_preps(first_info)
                    prep_meta = _decode_preps(encoded_preps)

                groups = []
                subgroup = None
                if second_info:
                    groups = [
                        _normalize_spaces(group_link.get_text(" ", strip=True))
                        for group_link in second_info.find_all("a", href=True)
                        if _normalize_spaces(group_link.get_text(" ", strip=True))
                    ]
                    second_info_text = _normalize_spaces(second_info.get_text(" ", strip=True))
                    subgroup_match = re.search(r"подгруппа\s*(\d+)", second_info_text, flags=re.IGNORECASE)
                    if subgroup_match:
                        subgroup = subgroup_match.group(1)

                if not groups and group_name:
                    groups = [group_name]

                class_aud = tail.find("div", class_="class-aud")
                auditories = []
                if class_aud:
                    aud_links = class_aud.find_all("a", href=True)
                    if aud_links:
                        auditories = [
                            _normalize_spaces(aud_link.get_text(" ", strip=True))
                            for aud_link in aud_links
                            if _normalize_spaces(aud_link.get_text(" ", strip=True))
                        ]
                    else:
                        aud_text = _normalize_spaces(class_aud.get_text(" ", strip=True))
                        if aud_text:
                            if aud_text.lower() == "онлайн":
                                aud_text = "онлайн"
                            auditories = [aud_text]
                if not auditories:
                    auditories = [""]

                prep_names = [prep_name for _, prep_name in prep_meta if prep_name]
                if not prep_names:
                    prep_names = [""]

                lesson_info = _build_info(lesson_type, subgroup)
                group_lesson = {
                    "time": class_time,
                    "week": week,
                    "name": lesson_name,
                    "aud": auditories,
                    "info": lesson_info,
                    "prep": prep_names,
                }
                _merge_group_lesson(day_to_lessons[day_name], group_lesson)

                events.append({
                    "day": day_name,
                    "time": class_time,
                    "week": week,
                    "name": lesson_name,
                    "info": lesson_info,
                    "aud": auditories,
                    "groups": groups,
                    "prep_meta": prep_meta,
                    "prep_names": prep_names,
                })

    schedule = []
    for day_name, lessons in day_to_lessons.items():
        _sort_day_lessons(lessons)
        schedule.append({
            "day": day_name,
            "lessons": lessons,
        })
    schedule = schedule_tools.days_in_right_order(schedule)

    return group_name, schedule, events


def _merge_teacher_lesson(day_lessons: List[Dict[str, Any]], lesson: Dict[str, Any]) -> None:
    for day_lesson in day_lessons:
        same_signature = (
            day_lesson["time"] == lesson["time"]
            and day_lesson["week"] == lesson["week"]
            and day_lesson["name"] == lesson["name"]
            and day_lesson["info"] == lesson["info"]
        )
        if not same_signature:
            continue

        if day_lesson["aud"] == lesson["aud"]:
            day_lesson["groups"] = _unique_preserve_order(day_lesson["groups"] + lesson["groups"])
            return
        if day_lesson["groups"] == lesson["groups"]:
            day_lesson["aud"] = _unique_preserve_order(day_lesson["aud"] + lesson["aud"])
            return
    day_lessons.append(lesson)


def _merge_aud_lesson(day_lessons: List[Dict[str, Any]], lesson: Dict[str, Any]) -> None:
    for day_lesson in day_lessons:
        same_signature = (
            day_lesson["time"] == lesson["time"]
            and day_lesson["week"] == lesson["week"]
            and day_lesson["name"] == lesson["name"]
            and day_lesson["info"] == lesson["info"]
        )
        if not same_signature:
            continue

        if day_lesson["prep"] == lesson["prep"]:
            day_lesson["groups"] = _unique_preserve_order(day_lesson["groups"] + lesson["groups"])
            return
        if day_lesson["groups"] == lesson["groups"]:
            day_lesson["prep"] = _unique_preserve_order(day_lesson["prep"] + lesson["prep"])
            return
    day_lessons.append(lesson)


def build_teacher_and_auditory_schedules(
    events: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    teachers_map: Dict[Tuple[int, str], Dict[str, Any]] = {}
    auditories_map: Dict[str, Dict[str, Any]] = {}

    for event in events:
        if event["name"] == "свободно":
            continue

        event_groups = _unique_preserve_order([group for group in event.get("groups", []) if group]) or [""]
        event_aud = _unique_preserve_order([aud for aud in event.get("aud", []) if aud is not None])
        if not event_aud:
            event_aud = [""]

        prep_meta = [(prep_id, prep_name) for prep_id, prep_name in event.get("prep_meta", []) if prep_name]
        if not prep_meta:
            prep_meta = [(None, prep_name) for prep_name in event.get("prep_names", []) if prep_name]

        for raw_prep_id, prep_name in prep_meta:
            prep_id = raw_prep_id
            if prep_id is None:
                prep_id = 900000000 + (zlib.crc32(prep_name.encode("utf-8")) % 100000000)

            teacher_key = (prep_id, prep_name)
            if teacher_key not in teachers_map:
                teachers_map[teacher_key] = {
                    "prep": prep_name,
                    "prep_short_name": prep_name,
                    "pg_id": prep_id,
                    "days": {},
                }

            day_lessons = teachers_map[teacher_key]["days"].setdefault(event["day"], [])
            _merge_teacher_lesson(day_lessons, {
                "time": event["time"],
                "week": event["week"],
                "name": event["name"],
                "aud": event_aud,
                "info": event["info"],
                "groups": event_groups,
            })

        prep_names = [prep_name for _, prep_name in prep_meta] or [""]
        for aud in event_aud:
            if not aud or aud.lower() == "онлайн":
                continue

            if aud not in auditories_map:
                auditories_map[aud] = {
                    "aud": aud,
                    "days": {},
                }

            day_lessons = auditories_map[aud]["days"].setdefault(event["day"], [])
            _merge_aud_lesson(day_lessons, {
                "time": event["time"],
                "week": event["week"],
                "name": event["name"],
                "info": event["info"],
                "prep": prep_names,
                "groups": event_groups,
            })

    teacher_docs = []
    prepods = []
    for teacher in teachers_map.values():
        schedule = []
        for day_name, lessons in teacher["days"].items():
            _sort_day_lessons(lessons)
            schedule.append({
                "day": day_name,
                "lessons": lessons,
            })
        schedule = schedule_tools.days_in_right_order(schedule)

        teacher_doc = {
            "prep": teacher["prep"],
            "prep_short_name": teacher["prep_short_name"],
            "pg_id": teacher["pg_id"],
            "schedule": schedule,
        }
        teacher_docs.append(teacher_doc)
        prepods.append({
            "prep": teacher["prep"],
            "prep_short_name": teacher["prep_short_name"],
            "prep_id": teacher["pg_id"],
        })

    auditory_docs = []
    for auditory in auditories_map.values():
        schedule = []
        for day_name, lessons in auditory["days"].items():
            _sort_day_lessons(lessons)
            schedule.append({
                "day": day_name,
                "lessons": lessons,
            })
        schedule = schedule_tools.days_in_right_order(schedule)
        auditory_docs.append({
            "aud": auditory["aud"],
            "schedule": schedule,
        })

    teacher_docs = sorted(teacher_docs, key=lambda item: (item["prep"], item["pg_id"]))
    auditory_docs = sorted(auditory_docs, key=lambda item: item["aud"])
    prepods = sorted(prepods, key=lambda item: (item["prep"], item["prep_id"]))

    return teacher_docs, auditory_docs, prepods


class ISTUScheduleParser:
    def __init__(self):
        self.base_url = os.environ.get("ISTU_SCHEDULE_URL", DEFAULT_BASE_URL)
        self.timeout_sec = float(os.environ.get("ISTU_REQUEST_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
        self.retries = int(os.environ.get("ISTU_REQUEST_RETRIES", DEFAULT_RETRIES))
        self.max_workers = int(os.environ.get("ISTU_MAX_WORKERS", DEFAULT_MAX_WORKERS))
        self.min_success_rate = float(os.environ.get("ISTU_MIN_SUCCESS_RATE", DEFAULT_MIN_SUCCESS_RATE))
        self.groups_limit = int(os.environ.get("ISTU_GROUPS_LIMIT", 0))
        self.request_delay_sec = float(os.environ.get("ISTU_REQUEST_DELAY_SEC", 0))

    def _fetch_html(self, params: Optional[Dict[str, Any]] = None) -> str:
        last_error = None
        for attempt in range(self.retries + 1):
            try:
                response = requests.get(
                    url=self.base_url,
                    params=params or {},
                    timeout=self.timeout_sec,
                    headers={"User-Agent": "SmartScheduleIRNITUBot/1.0"},
                )
                response.raise_for_status()
                if self.request_delay_sec > 0:
                    import time
                    time.sleep(self.request_delay_sec)
                return response.text
            except requests.RequestException as error:
                last_error = error
                logger.warning(
                    f"Failed to fetch ISTU page (attempt {attempt + 1}/{self.retries + 1}, params={params}): {error}"
                )
        raise RuntimeError(f"Could not fetch ISTU page for params={params}: {last_error}")

    def _parse_group_page(self, group: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        group_id = group["group_id"]
        html = self._fetch_html(params={"group": group_id})
        group_name, schedule, events = parse_group_schedule_html(html=html, fallback_group_name=group["name"])
        return {
            "group": group_name,
            "schedule": schedule,
        }, events

    def parse(self) -> Dict[str, List[Dict[str, Any]]]:
        logger.info("Start parsing ISTU schedule website...")

        main_html = self._fetch_html()
        subdivisions = parse_subdivisions_html(main_html)
        if not subdivisions:
            raise RuntimeError("No subdivisions found on ISTU schedule page")

        institutes = [{"name": subdivision["institute"]} for subdivision in subdivisions]

        groups = []
        for subdivision in subdivisions:
            subdivision_html = self._fetch_html(params={"subdiv": subdivision["subdiv_id"]})
            groups.extend(parse_groups_html(subdivision_html, subdivision["institute"]))

        if not groups:
            raise RuntimeError("No groups found while parsing ISTU subdivisions")

        groups_by_id = {}
        for group in groups:
            groups_by_id[group["group_id"]] = group
        groups = sorted(groups_by_id.values(), key=lambda item: item["name"])

        if self.groups_limit > 0:
            groups = groups[:self.groups_limit]

        courses = []
        seen_courses = set()
        for group in groups:
            course_key = (group["course"], group["institute"])
            if course_key in seen_courses:
                continue
            seen_courses.add(course_key)
            courses.append({
                "name": group["course"],
                "institute": group["institute"],
            })

        group_docs = []
        all_events = []
        failed_groups = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._parse_group_page, group): group
                for group in groups
            }
            for future in as_completed(futures):
                group = futures[future]
                try:
                    group_doc, events = future.result()
                    group_docs.append(group_doc)
                    all_events.extend(events)
                except Exception as error:
                    failed_groups.append(group["name"])
                    logger.warning(f"Failed to parse group {group['name']} (id={group['group_id']}): {error}")

        success_count = len(group_docs)
        total_groups = len(groups)
        success_rate = success_count / total_groups if total_groups else 0
        if success_rate < self.min_success_rate:
            raise RuntimeError(
                f"Too many failed groups while parsing ISTU website: {success_count}/{total_groups} successful"
            )

        if failed_groups:
            logger.warning(f"Failed groups count: {len(failed_groups)}")

        group_docs_by_name = {group_doc["group"]: group_doc for group_doc in group_docs}
        for group in groups:
            if group["name"] in group_docs_by_name:
                continue
            group_docs_by_name[group["name"]] = {
                "group": group["name"],
                "schedule": [],
            }
        group_docs = sorted(group_docs_by_name.values(), key=lambda item: item["group"])

        teacher_docs, auditory_docs, prepods = build_teacher_and_auditory_schedules(all_events)

        logger.info(
            f"ISTU parsing completed: institutes={len(institutes)}, groups={len(groups)}, "
            f"group_schedules={len(group_docs)}, teachers={len(teacher_docs)}, auditories={len(auditory_docs)}"
        )

        return {
            "institutes": institutes,
            "courses": sorted(courses, key=lambda item: (item["institute"], item["name"])),
            "groups": [{"name": group["name"], "course": group["course"], "institute": group["institute"]}
                       for group in groups],
            "schedule": group_docs,
            "prepods": prepods,
            "prepods_schedule": teacher_docs,
            "auditories_schedule": auditory_docs,
        }
