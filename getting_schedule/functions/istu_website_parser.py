import os
import re
import time
import zlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

from functions import schedule_tools
from functions.logger import logger


DEFAULT_BASE_URL = "https://www.istu.edu/raspisanie/"
DEFAULT_TIMEOUT_SEC = 20
DEFAULT_RETRIES = 2
DEFAULT_MAX_WORKERS = 8
DEFAULT_MIN_SUCCESS_RATE = 0.7
DEFAULT_PROGRESS_UPDATES = 20
DEFAULT_MARKER_RETRIES = 1
DEFAULT_MARKER_RETRY_DELAY_SEC = 1
DEFAULT_DETAILED_LOGS_ENABLED = False
DEFAULT_PROGRESS_LOGS_ENABLED = True


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_spaces(value: str) -> str:
    return " ".join(value.split()) if value else ""


def _parse_path_int(href: str, segment: str) -> Optional[int]:
    match = re.search(r"/" + re.escape(segment) + r"/(\d+)", href or "")
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
    if "week-all" in classes_set:
        return "all"
    if "week-even" in classes_set:
        return "even"
    if "week-odd" in classes_set:
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

    for anchor in soup.select('a[href*="/podrazdelenie/"]'):
        href = anchor.get("href", "")
        subdiv_id = _parse_path_int(href, "podrazdelenie")
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

    for course_block in soup.find_all("div", class_="schd-kurs-block"):
        course_tag = course_block.find("div", class_="schd-kurs-nuber")
        course_name = _normalize_course_name(
            course_tag.get_text(" ", strip=True) if course_tag else ""
        )

        for anchor in course_block.select('.schd-kurs-groups a[href*="/grup/"]'):
            href = anchor.get("href", "")
            group_id = _parse_path_int(href, "grup")
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
    for anchor in soup.select('a[href*="/grup/"]'):
        href = anchor.get("href", "")
        group_id = _parse_path_int(href, "grup")
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
    heading = soup.find("h1")
    if heading:
        text = _normalize_spaces(heading.get_text(" ", strip=True))
        match = re.match(r"(?:группа|group)\s*[:\-]?\s*(.+)", text, flags=re.IGNORECASE)
        if match:
            group_name = match.group(1).strip()
            if group_name:
                return group_name

    return fallback_group_name


def _extract_preps(prepod_tag: Optional[Tag]) -> List[Tuple[Optional[int], str]]:
    preps = []
    if not prepod_tag:
        return preps

    for prep_link in prepod_tag.find_all("a", href=True):
        prep_name = _normalize_spaces(prep_link.get_text(" ", strip=True))
        if not prep_name:
            continue
        prep_id = _parse_path_int(prep_link["href"], "prepodavatel")
        preps.append((prep_id, prep_name))

    return preps


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


def _find_schedule_container(soup: BeautifulSoup) -> Optional[Tag]:
    schedule_container = soup.select_one("div.sch-list-week")
    if schedule_container:
        return schedule_container

    main_content = soup.select_one("main.page-content")
    if main_content:
        return main_content

    return soup


def _html_debug_summary(html: str) -> str:
    html_lower = html.lower()
    markers = {
        "sch_list_day": 'sch-list-day' in html_lower,
        "sch_list_item": 'sch-list-item' in html_lower,
        "schcls_item": 'schcls-item' in html_lower,
        "week_even": 'week-even' in html_lower,
        "week_odd": 'week-odd' in html_lower,
    }
    soup = BeautifulSoup(html, "html.parser")
    title = _normalize_spaces(soup.title.get_text(" ", strip=True)) if soup.title else ""
    body_preview = _normalize_spaces(soup.get_text(" ", strip=True))
    preview = body_preview[:300]
    markers_str = ", ".join(f"{key}={value}" for key, value in markers.items())
    return f"{markers_str}, title={title}, preview={preview}"


def _html_has_schedule_markers(html: str) -> bool:
    html_lower = html.lower()
    return any(
        marker in html_lower
        for marker in (
            "sch-list-day",
            "sch-list-item",
            "schcls-item",
        )
    )


def parse_group_schedule_html(
    html: str,
    fallback_group_name: str,
    detailed_logs_enabled: bool = DEFAULT_DETAILED_LOGS_ENABLED,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    soup = BeautifulSoup(html, "html.parser")
    group_name = _extract_group_name(soup, fallback_group_name)

    schedule_container = _find_schedule_container(soup)
    if not schedule_container:
        if detailed_logs_enabled:
            logger.warning(
                f'Group "{group_name}": schedule container not found on ISTU page; '
                f'{_html_debug_summary(html)}'
            )
        return group_name, [], []

    day_to_lessons: Dict[str, List[Dict[str, Any]]] = {}
    events: List[Dict[str, Any]] = []
    valid_days = set(schedule_tools.DAYS.values())
    day_heading_candidates = 0
    valid_day_headings = 0
    recognized_week_blocks = 0

    for day_block in schedule_container.find_all("div", class_="sch-list-day"):
        day_heading_candidates += 1
        day_heading = day_block.find(class_="sch-list-day-header")
        day_heading_text = _normalize_spaces(day_heading.get_text(" ", strip=True)) if day_heading else ""
        if "," not in day_heading_text:
            continue

        day_name = day_heading_text.split(",", 1)[0].strip().lower()
        if day_name not in valid_days:
            continue
        valid_day_headings += 1

        if day_name not in day_to_lessons:
            day_to_lessons[day_name] = []

        for schedule_item in day_block.find_all("div", class_="sch-list-item", recursive=False):
            time_tag = schedule_item.select_one(".sch-list-item-time-inner")
            class_time = _normalize_spaces(time_tag.get_text(" ", strip=True)) if time_tag else ""
            if not class_time:
                continue

            classes_container = schedule_item.find("div", class_="sch-list-item-classes")
            if not classes_container:
                continue

            for week_block in classes_container.find_all("div", class_="sch-list-item-week", recursive=False):
                week = _normalize_week(week_block.get("class", []))
                if not week:
                    continue
                recognized_week_blocks += 1

                for card in week_block.find_all("div", class_="schcls-item", recursive=False):
                    if "schcls-empty" in card.get("class", []):
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

                    info = card.find("div", class_="schcls-item-info")
                    if not info:
                        continue

                    name_tag = info.find("div", class_="schcls-item-name")
                    lesson_name = _normalize_spaces(name_tag.get_text(" ", strip=True)) if name_tag else ""
                    if not lesson_name:
                        continue

                    distype_tag = info.find("div", class_="schcls-item-distype")
                    lesson_type = _normalize_spaces(distype_tag.get_text(" ", strip=True)) if distype_tag else ""

                    prepod_tag = info.find("div", class_="schcls-item-prepod")
                    prep_meta = _extract_preps(prepod_tag)

                    group_tag = info.find("div", class_="schcls-item-group")
                    groups = []
                    subgroup = None
                    if group_tag:
                        groups = [
                            _normalize_spaces(group_link.get_text(" ", strip=True))
                            for group_link in group_tag.find_all("a", href=True)
                            if _normalize_spaces(group_link.get_text(" ", strip=True))
                        ]
                        group_text = _normalize_spaces(group_tag.get_text(" ", strip=True))
                        subgroup_match = re.search(r"подгруппа\s*(\d+)", group_text, flags=re.IGNORECASE)
                        if subgroup_match:
                            subgroup = subgroup_match.group(1)

                    if not groups and group_name:
                        groups = [group_name]

                    aud_tag = card.find("div", class_="schcls-item-aud")
                    auditories = []
                    if aud_tag:
                        aud_links = aud_tag.find_all("a", href=True)
                        if aud_links:
                            auditories = [
                                _normalize_spaces(aud_link.get_text(" ", strip=True))
                                for aud_link in aud_links
                                if _normalize_spaces(aud_link.get_text(" ", strip=True))
                            ]
                        else:
                            aud_text = _normalize_spaces(aud_tag.get_text(" ", strip=True))
                            if aud_text:
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

    if not schedule:
        if day_heading_candidates == 0:
            empty_reason = "no day blocks inside schedule container"
        elif valid_day_headings == 0:
            empty_reason = "no valid weekday headings recognized"
        elif recognized_week_blocks == 0:
            empty_reason = "no lesson slots with supported week markers found"
        else:
            empty_reason = "page parsed but zero lessons were extracted"
        if detailed_logs_enabled:
            logger.warning(f'Group "{group_name}": parsed empty schedule, reason={empty_reason}')

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
    def __init__(self, progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None):
        self.base_url = os.environ.get("ISTU_SCHEDULE_URL", DEFAULT_BASE_URL)
        self.timeout_sec = float(os.environ.get("ISTU_REQUEST_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
        self.retries = int(os.environ.get("ISTU_REQUEST_RETRIES", DEFAULT_RETRIES))
        self.max_workers = int(os.environ.get("ISTU_MAX_WORKERS", DEFAULT_MAX_WORKERS))
        self.min_success_rate = float(os.environ.get("ISTU_MIN_SUCCESS_RATE", DEFAULT_MIN_SUCCESS_RATE))
        self.groups_limit = int(os.environ.get("ISTU_GROUPS_LIMIT", 0))
        self.request_delay_sec = float(os.environ.get("ISTU_REQUEST_DELAY_SEC", 0))
        self.marker_retries = int(os.environ.get("ISTU_MARKER_RETRIES", DEFAULT_MARKER_RETRIES))
        self.marker_retry_delay_sec = float(
            os.environ.get("ISTU_MARKER_RETRY_DELAY_SEC", DEFAULT_MARKER_RETRY_DELAY_SEC)
        )
        self.detailed_logs_enabled = _env_flag("ISTU_DETAILED_LOGS", DEFAULT_DETAILED_LOGS_ENABLED)
        self.progress_logs_enabled = _env_flag("ISTU_PROGRESS_LOGS", DEFAULT_PROGRESS_LOGS_ENABLED)
        self.progress_callback = progress_callback
        self._thread_local = local()

    def _log_detailed_warning(self, message: str) -> None:
        if self.detailed_logs_enabled:
            logger.warning(message)

    def _log_detailed_info(self, message: str) -> None:
        if self.detailed_logs_enabled:
            logger.info(message)

    def _emit_progress(self, stage: str, **payload: Any) -> None:
        if not self.progress_callback:
            return
        try:
            self.progress_callback(stage, payload)
        except Exception as error:
            logger.warning(f"Failed to publish parser progress for stage={stage}: {error}")

    def _log_group_progress(self, completed: int, total: int, successful: int, failed: int) -> None:
        progress_percent = int((completed / total) * 100) if total else 100
        if self.progress_logs_enabled:
            logger.info(
                "ISTU parsing progress: "
                f"{completed}/{total} groups ({progress_percent}%), "
                f"successful={successful}, failed={failed}"
            )
        self._emit_progress(
            "parsing_group_pages",
            total_groups=total,
            completed_groups=completed,
            successful_groups=successful,
            failed_groups=failed,
            progress_percent=progress_percent,
        )

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            })
            self._thread_local.session = session
        return session

    def _fetch_page(
        self,
        params: Optional[Dict[str, Any]] = None,
        base_url: Optional[str] = None,
    ) -> Tuple[str, str]:
        last_error = None
        request_url = base_url or self.base_url
        session = self._get_session()
        for attempt in range(self.retries + 1):
            try:
                response = session.get(
                    url=request_url,
                    params=params or {},
                    timeout=self.timeout_sec,
                )
                response.raise_for_status()
                if self.request_delay_sec > 0:
                    time.sleep(self.request_delay_sec)
                return response.text, response.url
            except requests.RequestException as error:
                last_error = error
                logger.warning(
                    f"Failed to fetch ISTU page (attempt {attempt + 1}/{self.retries + 1}, "
                    f"url={request_url}, params={params}): {error}"
                )
        raise RuntimeError(f"Could not fetch ISTU page for url={request_url}, params={params}: {last_error}")

    def _fetch_html(self, params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None) -> str:
        html, _ = self._fetch_page(params=params, base_url=base_url)
        return html

    def _group_schedule_url(self, group_id: int, day_offset: Optional[int] = None) -> str:
        url = f"{self.base_url.rstrip('/')}/grup/{group_id}"
        if day_offset is not None:
            target_date = datetime.now() + timedelta(days=day_offset)
            url += "/" + target_date.strftime("%d.%m.%Y")
        return url

    def _build_group_request_variants(self, group_id: int) -> List[Tuple[str, Dict[str, Any], str]]:
        # ISTU renders the schedule per calendar week and only includes weekdays that
        # actually have lessons, so a week near a semester break can come back empty.
        # Probe a few weeks around "today" until one has data.
        variants = [
            (self._group_schedule_url(group_id), {}, "current_week"),
            (self._group_schedule_url(group_id, day_offset=7), {}, "next_week"),
            (self._group_schedule_url(group_id, day_offset=-7), {}, "previous_week"),
            (self._group_schedule_url(group_id, day_offset=14), {}, "two_weeks_ahead"),
        ]
        return variants

    def _parse_group_page(self, group: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        group_id = group["group_id"]
        html = ""
        request_label = "group_only"
        request_url = self.base_url
        request_params = {"group": group_id}
        response_url = self.base_url
        for variant_url, variant_params, variant_label in self._build_group_request_variants(group_id):
            marker_found = False
            for marker_attempt in range(self.marker_retries + 1):
                html, response_url = self._fetch_page(params=variant_params, base_url=variant_url)
                if _html_has_schedule_markers(html):
                    marker_found = True
                    break

                if marker_attempt < self.marker_retries:
                    self._log_detailed_warning(
                        f'Group "{group["name"]}" (id={group_id}): '
                        f'no schedule markers on attempt {marker_attempt + 1}/{self.marker_retries + 1} '
                        f'for variant={variant_label}, requested_url={variant_url}, '
                        f'response_url={response_url}, params={variant_params}; retrying after '
                        f'{self.marker_retry_delay_sec} sec'
                    )
                    if self.marker_retry_delay_sec > 0:
                        time.sleep(self.marker_retry_delay_sec)
            request_label = variant_label
            request_url = variant_url
            request_params = variant_params
            if marker_found:
                if variant_label != "group_only":
                    self._log_detailed_info(
                        f'Group "{group["name"]}" (id={group_id}): '
                        f'schedule markers found via fallback {variant_label}; '
                        f'response_url={response_url}'
                    )
                break

            self._log_detailed_warning(
                f'Group "{group["name"]}" (id={group_id}): no schedule markers for '
                f'variant={variant_label}, requested_url={variant_url}, '
                f'response_url={response_url}, params={variant_params}; {_html_debug_summary(html)}'
            )

        group_name, schedule, events = parse_group_schedule_html(
            html=html,
            fallback_group_name=group["name"],
            detailed_logs_enabled=self.detailed_logs_enabled,
        )
        if not schedule:
            self._log_detailed_warning(
                f'Group "{group_name}" (id={group_id}) produced empty schedule after parsing; '
                f'events={len(events)}; variant={request_label}, requested_url={request_url}, '
                f'response_url={response_url}, '
                f'params={request_params}; {_html_debug_summary(html)}'
            )
        return {
            "group": group_name,
            "schedule": schedule,
        }, events

    def parse(self) -> Dict[str, List[Dict[str, Any]]]:
        logger.info("Start parsing ISTU schedule website...")
        self._emit_progress("fetching_main_page")

        main_html = self._fetch_html()
        subdivisions = parse_subdivisions_html(main_html)
        groups = []
        if subdivisions:
            institutes = [{"name": subdivision["institute"]} for subdivision in subdivisions]
            logger.info(f"ISTU parser: found {len(subdivisions)} subdivisions, loading group lists...")
            self._emit_progress(
                "fetching_subdivisions",
                subdivisions_total=len(subdivisions),
            )
            for subdivision in subdivisions:
                subdivision_url = f"{self.base_url.rstrip('/')}/podrazdelenie/{subdivision['subdiv_id']}"
                subdivision_html = self._fetch_html(base_url=subdivision_url)
                groups.extend(parse_groups_html(subdivision_html, subdivision["institute"]))
        else:
            logger.warning("No subdivisions found on ISTU main page. Trying fallback parsing from main page.")
            fallback_institute = "ИРНИТУ"
            groups = parse_groups_html(main_html, fallback_institute)
            institutes = [{"name": fallback_institute}] if groups else []

        if not groups:
            raise RuntimeError("No groups found while parsing ISTU subdivisions")

        groups_by_id = {}
        for group in groups:
            groups_by_id[group["group_id"]] = group
        groups = sorted(groups_by_id.values(), key=lambda item: item["name"])

        if self.groups_limit > 0:
            groups = groups[:self.groups_limit]

        logger.info(
            f"ISTU parser: collected {len(groups)} groups from {len(institutes)} institutes. "
            f"Starting parsing with {self.max_workers} workers..."
        )
        self._emit_progress(
            "preparing_group_pages",
            institutes_total=len(institutes),
            total_groups=len(groups),
            max_workers=self.max_workers,
        )

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
        total_groups = len(groups)
        progress_step = max(1, total_groups // DEFAULT_PROGRESS_UPDATES) if total_groups else 1

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
                    failed_groups.append(f"{group['name']} (id={group['group_id']})")
                    self._log_detailed_warning(
                        f"Failed to parse group {group['name']} (id={group['group_id']}): {error}"
                    )

                completed_groups = len(group_docs) + len(failed_groups)
                if completed_groups == total_groups or completed_groups % progress_step == 0:
                    self._log_group_progress(
                        completed=completed_groups,
                        total=total_groups,
                        successful=len(group_docs),
                        failed=len(failed_groups),
                    )

        success_count = len(group_docs)
        success_rate = success_count / total_groups if total_groups else 0
        if success_rate < self.min_success_rate:
            logger.warning(
                "Low ISTU parse success rate: "
                f"{success_count}/{total_groups} successful (threshold={self.min_success_rate}). "
                "Saving available data anyway."
            )

        if failed_groups:
            preview = ", ".join(failed_groups[:10])
            logger.warning(f"Failed groups count: {len(failed_groups)}. Examples: {preview}")

        group_docs_by_name = {group_doc["group"]: group_doc for group_doc in group_docs}
        for group in groups:
            if group["name"] in group_docs_by_name:
                continue
            group_docs_by_name[group["name"]] = {
                "group": group["name"],
                "schedule": [],
            }
        group_docs = sorted(group_docs_by_name.values(), key=lambda item: item["group"])
        empty_schedule_groups = [group_doc["group"] for group_doc in group_docs if not group_doc["schedule"]]
        if empty_schedule_groups:
            preview = ", ".join(empty_schedule_groups[:10])
            logger.warning(
                f"ISTU parser: empty schedules for {len(empty_schedule_groups)} groups. "
                f"Examples: {preview}"
            )

        logger.info("ISTU parser: building derived teachers and auditories schedules...")
        self._emit_progress(
            "building_derived_schedules",
            total_groups=total_groups,
            successful_groups=success_count,
            failed_groups=len(failed_groups),
            empty_schedule_groups=len(empty_schedule_groups),
        )
        teacher_docs, auditory_docs, prepods = build_teacher_and_auditory_schedules(all_events)

        logger.info(
            f"ISTU parsing completed: institutes={len(institutes)}, groups={len(groups)}, "
            f"group_schedules={len(group_docs)}, teachers={len(teacher_docs)}, auditories={len(auditory_docs)}"
        )
        self._emit_progress(
            "completed",
            institutes_total=len(institutes),
            total_groups=len(groups),
            successful_groups=success_count,
            failed_groups=len(failed_groups),
            empty_schedule_groups=len(empty_schedule_groups),
            group_schedules=len(group_docs),
            teacher_schedules=len(teacher_docs),
            auditories_schedules=len(auditory_docs),
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
