"""Общие функции сборки расписания: нормализация пар, слияние дублей и построение
расписаний преподавателей и аудиторий из событий групп."""
import zlib
from typing import Any, Dict, List, Optional, Tuple

from functions import schedule_tools


DEFAULT_TIMEOUT_SEC = 20
DEFAULT_RETRIES = 2
DEFAULT_MAX_WORKERS = 8
DEFAULT_MIN_SUCCESS_RATE = 0.7
DEFAULT_PROGRESS_UPDATES = 20


def _normalize_spaces(value: str) -> str:
    return " ".join(value.split()) if value else ""


def _unique_preserve_order(items: List[str]) -> List[str]:
    result = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


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
