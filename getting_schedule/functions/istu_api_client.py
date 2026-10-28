"""Клиент официального API расписания ИРНИТУ (https://schedule.istu.edu/api/).

Единственный источник расписания (парсинг сайта istu.edu/raspisanie удалён):
``parse()`` отдаёт институты, группы, курсы, преподавателей и расписания групп,
преподавателей и аудиторий в формате, который сохраняет main.py.

Устройство API (выведено из работы веб-интерфейса, документации нет):
  * авторизация — заголовок ``Authorization: Token <ключ>``;
  * ``GET /api/group/``, ``/api/teacher/``, ``/api/auditory/`` — справочники;
  * ``GET /api/group/<id>/schedule/?dbeg=YYYY-MM-DD&dend=YYYY-MM-DD`` — расписание
    ровно одной календарной недели (понедельник..воскресенье). Для диапазона
    длиннее недели приходят только разовые переносы.
  * ``everyweek == 2`` — пара каждую неделю, ``everyweek == 1`` — только на неделях
    той чётности, на которую пришёлся запрос; поэтому, чтобы собрать «чётную» и
    «нечётную» недели, запрашиваются две соседние недели.
  * поле ``week_even`` в ответе инвертировано относительно принятого в проекте
    счёта недель (см. functions_api/functions/find_week.py), поэтому чётность
    считается по дате, а не берётся из ответа.
"""
import html
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import local
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytz
import requests

from functions import schedule_tools
from functions.schedule_builder import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_MIN_SUCCESS_RATE,
    DEFAULT_PROGRESS_UPDATES,
    DEFAULT_TIMEOUT_SEC,
    DEFAULT_RETRIES,
    _build_info,
    _merge_group_lesson,
    _normalize_spaces,
    _sort_day_lessons,
    build_teacher_and_auditory_schedules,
)
from functions.transfers import (
    DAY_NAMES,
    QUERY_TYPE_TRANSFER,
    add_transfer,
    find_original_lesson,
    is_transfer,
    make_transfer_record,
    parse_transfer_title,
    transfer_target_auditories,
)
from functions.logger import logger

DEFAULT_API_URL = "https://schedule.istu.edu/api"
TZ_IRKUTSK = pytz.timezone("Asia/Irkutsk")

# Время начала пар по номеру (para) — таблица из веб-интерфейса schedule.istu.edu.
PARA_TIMES = {
    1: "8:15",
    2: "10:00",
    3: "11:45",
    4: "13:45",
    5: "15:30",
    6: "17:10",
    7: "18:45",
    8: "20:20",
}

# nt -> вид занятия (передаётся в _build_info, который нормализует его до "Лекция"/"Практ."/"Лаб. раб.").
LESSON_TYPES = {1: "лекция", 2: "практика", 3: "лабораторная"}

# Сколько соседних недель пробуем, если неделя нужной чётности оказалась пустой
# (каникулы, начало семестра).
WEEK_SEARCH_RADIUS = 3

# На какой срок вперёд собираем разовые переносы (запрос с диапазоном длиннее недели
# возвращает только их, без регулярных пар).
QUERY_HORIZON_DAYS = 120

DEFAULT_INSTITUTE = "ИРНИТУ"


def week_parity_of(monday: date) -> str:
    """Чётность недели с понедельником ``monday`` ('even' / 'odd').

    Неделя, на которую приходится 1 сентября, — чётная, дальше чётность чередуется.
    Должно совпадать с find_week() в functions_api (см. память проекта про чётность).
    """
    sep_year = monday.year if monday.month >= 9 else monday.year - 1
    sep = date(sep_year, 9, 1)
    week0_monday = sep - timedelta(days=sep.weekday())
    return "odd" if ((monday - week0_monday).days // 7) % 2 else "even"


def _monday_of(day: date) -> date:
    return day - timedelta(days=day.weekday())


def candidate_weeks(today: date) -> Dict[str, List[date]]:
    """Понедельники, которые стоит запросить для каждой чётности: ближайшие к сегодняшнему дню первыми."""
    current = _monday_of(today)
    offsets = [0]
    for step in range(1, WEEK_SEARCH_RADIUS + 1):
        offsets.extend([step, -step])

    result: Dict[str, List[date]] = {"even": [], "odd": []}
    for offset in offsets:
        monday = current + timedelta(weeks=offset)
        result[week_parity_of(monday)].append(monday)
    return result


def _has_transfers(payload: Dict[str, Any]) -> bool:
    return any(query.get("type") == QUERY_TYPE_TRANSFER for query in payload.get("queries") or [])


class ISTUApiClient:
    def __init__(self, progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None):
        self.api_key = os.environ.get("ISTU_API_KEY", "").strip()
        if not self.api_key:
            raise RuntimeError("ISTU_API_KEY is not set")

        self.base_url = os.environ.get("ISTU_API_URL", DEFAULT_API_URL).rstrip("/")
        self.timeout_sec = float(os.environ.get("ISTU_REQUEST_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
        self.retries = int(os.environ.get("ISTU_REQUEST_RETRIES", DEFAULT_RETRIES))
        self.max_workers = int(os.environ.get("ISTU_MAX_WORKERS", DEFAULT_MAX_WORKERS))
        self.min_success_rate = float(os.environ.get("ISTU_MIN_SUCCESS_RATE", DEFAULT_MIN_SUCCESS_RATE))
        self.groups_limit = int(os.environ.get("ISTU_GROUPS_LIMIT", 0))
        self.progress_callback = progress_callback
        self._thread_local = local()

    # --- HTTP -------------------------------------------------------------

    def _emit_progress(self, stage: str, **payload: Any) -> None:
        if not self.progress_callback:
            return
        try:
            self.progress_callback(stage, payload)
        except Exception as error:
            logger.warning(f"Failed to publish API client progress for stage={stage}: {error}")

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "Authorization": f"Token {self.api_key}",
                "Accept": "application/json",
            })
            self._thread_local.session = session
        return session

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_error = None
        session = self._get_session()
        for attempt in range(self.retries + 1):
            try:
                response = session.get(url, params=params or {}, timeout=self.timeout_sec)
                if response.status_code in (401, 403):
                    # Ретраи бесполезны: ключ неверный/просрочен. Текст ответа не логируем.
                    raise PermissionError(f"ISTU API rejected the key (HTTP {response.status_code}) for {url}")
                response.raise_for_status()
                return response.json()
            except PermissionError:
                raise
            except (requests.RequestException, ValueError) as error:
                last_error = error
                logger.warning(
                    f"Failed to fetch ISTU API (attempt {attempt + 1}/{self.retries + 1}, "
                    f"url={url}, params={params}): {error}"
                )
                time.sleep(min(2 ** attempt, 5))
        raise RuntimeError(f"Could not fetch ISTU API url={url}, params={params}: {last_error}")

    # --- Справочники ------------------------------------------------------

    def fetch_groups(self) -> List[Dict[str, Any]]:
        groups = []
        for item in self._get_json("group/"):
            title = _normalize_spaces(item.get("title") or "")
            if not title:
                continue
            kurs = item.get("kurs")
            groups.append({
                "group_id": item["id"],
                "name": title,
                "course": f"{kurs} курс" if kurs else "1 курс",
                "institute": _normalize_spaces(item.get("faculty_title") or "") or DEFAULT_INSTITUTE,
            })
        return groups

    def fetch_teachers(self) -> Dict[int, str]:
        teachers = {}
        for item in self._get_json("teacher/"):
            name = _normalize_spaces(item.get("name") or item.get("full_name") or "")
            if name:
                teachers[item["id"]] = name
        return teachers

    def fetch_auditories(self) -> Dict[int, str]:
        auditories = {}
        for item in self._get_json("auditory/"):
            title = _normalize_spaces(item.get("title") or "")
            if title and title != "-":
                auditories[item["id"]] = title
        return auditories

    # --- Расписание группы ------------------------------------------------

    def _fetch_group_week(self, group_id: int, monday: date) -> Dict[str, Any]:
        return self._get_json(
            f"group/{group_id}/schedule/",
            params={
                "dbeg": monday.isoformat(),
                "dend": (monday + timedelta(days=6)).isoformat(),
                "with_projected": "false",
            },
        )

    def _fetch_group_transfers(self, group_id: int, today: date) -> Dict[str, Any]:
        """Разовые переносы на ближайшие месяцы одним запросом (диапазон > недели)."""
        monday = _monday_of(today)
        return self._get_json(
            f"group/{group_id}/schedule/",
            params={
                "dbeg": monday.isoformat(),
                "dend": (monday + timedelta(days=QUERY_HORIZON_DAYS)).isoformat(),
                "with_projected": "false",
            },
        )

    def _fetch_group_weeks(self, group_id: int, today: date) -> List[Tuple[str, date, Dict[str, Any]]]:
        """Для каждой чётности берём ближайшую неделю, где у группы есть пары."""
        weeks = []
        for parity, mondays in candidate_weeks(today).items():
            last_payload = None
            for monday in mondays:
                payload = self._fetch_group_week(group_id, monday)
                last_payload = (monday, payload)
                if payload.get("schedule"):
                    weeks.append((parity, monday, payload))
                    break
            else:
                if last_payload:
                    weeks.append((parity, last_payload[0], last_payload[1]))
        return weeks

    def _lesson_from_item(
        self,
        item: Dict[str, Any],
        week: str,
        monday: date,
        queries_by_id: Dict[int, Dict[str, Any]],
        group_titles: Dict[int, str],
        teacher_names: Dict[int, str],
        auditory_names: Dict[int, str],
        fallback_group_name: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            day_number = int(item.get("day"))
            para = int(item.get("para"))
        except (TypeError, ValueError):
            return None
        # day: 1..7 — дни недели одной чётности, 8..14 — второй (как в старой БД расписания);
        # чётность берём из запрошенной недели, а день недели — по модулю 7.
        day_number = (day_number - 1) % 7 + 1
        class_time = PARA_TIMES.get(para)
        day_name = schedule_tools.DAYS.get(day_number)
        title = _normalize_spaces(item.get("title") or "")
        if not class_time or not day_name or not title:
            return None

        subgroup = item.get("ngroup")
        transfer = None
        target_date = None
        target_auditories: List[str] = []
        if item.get("type") == "query":
            # Разовое занятие: проектная деятельность или перенос регулярной пары. Показываем
            # его только в свой день (поле date), а не каждую неделю этой чётности.
            title = html.unescape(title)
            query = queries_by_id.get(item.get("id"))
            target = query["dt"] if query and query.get("dt") else (monday + timedelta(days=day_number - 1)).isoformat()
            target_date = datetime.strptime(target, "%Y-%m-%d").date()
            week = week_parity_of(_monday_of(target_date))
            day_name = DAY_NAMES[target_date.weekday()]
            if is_transfer(query, title):
                transfer = parse_transfer_title(title)
                # Подгруппу берём из текста карточки ("2-ая подгруппа ..."): в ngroup её нет,
                # а вид занятия (nt) у переносов всегда "лекция".
                if transfer["subgroup"] is not None:
                    subgroup = transfer["subgroup"]
                target_auditories = transfer_target_auditories(query, auditory_names)
        lesson_info = _build_info(LESSON_TYPES.get(item.get("nt"), ""), str(subgroup) if subgroup else None)

        auditories = target_auditories or [
            auditory_names[aud_id] for aud_id in (item.get("auditories_ids") or []) if aud_id in auditory_names
        ]
        if not auditories:
            fallback_aud = _normalize_spaces(item.get("auditories_verbose") or item.get("auditories") or "")
            auditories = [fallback_aud] if fallback_aud else [""]

        prep_meta = [
            (teacher_id, teacher_names[teacher_id])
            for teacher_id in (item.get("teachers_ids") or [])
            if teacher_id in teacher_names
        ]
        prep_names = [name for _, name in prep_meta] or [""]

        groups = [group_titles[group_id] for group_id in (item.get("groups") or []) if group_id in group_titles]
        if not groups:
            groups = [fallback_group_name]

        return {
            "day": day_name,
            "time": class_time,
            "week": week,
            "name": title,
            "info": lesson_info,
            "aud": auditories,
            "groups": groups,
            "prep_meta": prep_meta,
            "prep_names": prep_names,
            "date": target_date.isoformat() if target_date else None,
            "transfer": transfer,
        }

    @staticmethod
    def _attach_transfer(
        event: Dict[str, Any],
        day_to_lessons: Dict[str, List[Dict[str, Any]]],
        events: List[Dict[str, Any]],
    ) -> bool:
        """Привязывает разовый перенос к исходной регулярной паре группы.

        Возвращает True, если пара нашлась: тогда у неё (и у её событий, из которых
        строятся расписания преподавателей и аудиторий) появляется запись в
        ``transfers``, а отдельной карточки переноса в расписании группы нет.
        """
        transfer = event["transfer"]
        if not transfer:
            return False
        target_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
        original_date = transfer["from_date"] or target_date
        original_time = PARA_TIMES.get(transfer["from_para"]) if transfer["from_para"] else None
        original = find_original_lesson(day_to_lessons, transfer, original_date, event["time"], original_time)
        if original is None:
            return False

        record = make_transfer_record(original_date, target_date, event["time"], event["aud"], transfer)
        add_transfer(original, record)
        for other in events:
            if other.get("date"):
                continue
            same_lesson = (
                other["day"] == DAY_NAMES[original_date.weekday()]
                and other["time"] == original["time"]
                and other["week"] == original["week"]
                and other["name"] == original["name"]
                and other["info"] == original["info"]
            )
            if same_lesson:
                add_transfer(other, record)

        # В расписании аудитории, куда переехала пара, она появляется разовым занятием.
        events.append({
            **event,
            "name": original["name"],
            "info": original["info"],
            "transfer": None,
            "aud_only": True,
        })
        return True

    def _parse_group(
        self,
        group: Dict[str, Any],
        today: date,
        group_titles: Dict[int, str],
        teacher_names: Dict[int, str],
        auditory_names: Dict[int, str],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        day_to_lessons: Dict[str, List[Dict[str, Any]]] = {}
        events: List[Dict[str, Any]] = []

        seen_regular = set()
        for parity, monday, payload in self._fetch_group_weeks(group["group_id"], today):
            payloads = [(monday, payload)]
            if _has_transfers(payload):
                # Пары, перенесённые на этой неделе, API из неё убирает. Чтобы такая пара
                # не пропала из регулярного расписания, добираем следующую неделю той же чётности.
                next_monday = monday + timedelta(weeks=2)
                payloads.append((next_monday, self._fetch_group_week(group["group_id"], next_monday)))

            for week_monday, week_payload in payloads:
                queries_by_id = {query["id"]: query for query in week_payload.get("queries") or []}
                for item in week_payload.get("schedule") or []:
                    if item.get("type") == "query":
                        continue  # разовые занятия собираются отдельным запросом ниже
                    week = "all" if item.get("everyweek") == 2 else parity
                    event = self._lesson_from_item(
                        item, week, week_monday, queries_by_id, group_titles, teacher_names, auditory_names,
                        group["name"],
                    )
                    if not event:
                        continue

                    # Еженедельная пара приходит в обоих запросах — оставляем одну копию.
                    signature = (event["day"], event["week"], event["time"], event["name"], event["info"],
                                 tuple(event["aud"]), tuple(event["prep_names"]))
                    if signature in seen_regular:
                        continue
                    seen_regular.add(signature)

                    events.append(event)
                    _merge_group_lesson(
                        day_to_lessons.setdefault(event["day"], []),
                        {
                            "time": event["time"],
                            "week": event["week"],
                            "name": event["name"],
                            "aud": event["aud"],
                            "info": event["info"],
                            "prep": event["prep_names"],
                        },
                    )

        transfers = self._fetch_group_transfers(group["group_id"], today)
        queries_by_id = {query["id"]: query for query in transfers.get("queries") or []}
        for item in transfers.get("schedule") or []:
            if item.get("type") != "query":
                continue
            event = self._lesson_from_item(
                item, "all", _monday_of(today), queries_by_id, group_titles, teacher_names, auditory_names,
                group["name"],
            )
            if not event:
                continue
            if self._attach_transfer(event, day_to_lessons, events):
                continue
            events.append(event)
            _merge_group_lesson(
                day_to_lessons.setdefault(event["day"], []),
                {
                    "time": event["time"],
                    "week": event["week"],
                    "name": event["name"],
                    "aud": event["aud"],
                    "info": event["info"],
                    "prep": event["prep_names"],
                    "date": event["date"],
                    **({"transfer": True} if event["transfer"] else {}),
                },
            )

        schedule = []
        for day_name, lessons in day_to_lessons.items():
            _sort_day_lessons(lessons)
            schedule.append({"day": day_name, "lessons": lessons})
        schedule = schedule_tools.days_in_right_order(schedule)

        return {"group": group["name"], "schedule": schedule}, events

    # --- Точка входа ------------------------------------------------------

    def parse(self) -> Dict[str, List[Dict[str, Any]]]:
        logger.info("Start loading ISTU schedule via API...")
        self._emit_progress("fetching_reference_data")

        groups = self.fetch_groups()
        teacher_names = self.fetch_teachers()
        auditory_names = self.fetch_auditories()
        if not groups:
            raise RuntimeError("ISTU API returned no groups")

        groups_by_name = {}
        for group in groups:
            groups_by_name[group["name"]] = group
        groups = sorted(groups_by_name.values(), key=lambda item: item["name"])
        if self.groups_limit > 0:
            groups = groups[:self.groups_limit]

        group_titles = {group["group_id"]: group["name"] for group in groups}
        institutes = [{"name": name} for name in sorted({group["institute"] for group in groups})]
        courses = sorted(
            [{"name": course, "institute": institute}
             for course, institute in {(group["course"], group["institute"]) for group in groups}],
            key=lambda item: (item["institute"], item["name"]),
        )

        logger.info(
            f"ISTU API: {len(groups)} groups, {len(teacher_names)} teachers, {len(auditory_names)} auditories. "
            f"Loading schedules with {self.max_workers} workers..."
        )
        self._emit_progress(
            "preparing_group_pages",
            institutes_total=len(institutes),
            total_groups=len(groups),
            max_workers=self.max_workers,
        )

        today = datetime.now(TZ_IRKUTSK).date()
        group_docs = []
        all_events = []
        failed_groups = []
        total_groups = len(groups)
        progress_step = max(1, total_groups // DEFAULT_PROGRESS_UPDATES)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._parse_group, group, today, group_titles, teacher_names, auditory_names): group
                for group in groups
            }
            for future in as_completed(futures):
                group = futures[future]
                try:
                    group_doc, events = future.result()
                    group_docs.append(group_doc)
                    all_events.extend(events)
                except PermissionError:
                    for pending in futures:
                        pending.cancel()
                    raise
                except Exception as error:
                    failed_groups.append(f"{group['name']} (id={group['group_id']})")
                    logger.warning(f"Failed to load group {group['name']} (id={group['group_id']}): {error}")

                completed = len(group_docs) + len(failed_groups)
                if completed == total_groups or completed % progress_step == 0:
                    logger.info(
                        f"ISTU API progress: {completed}/{total_groups} groups, "
                        f"successful={len(group_docs)}, failed={len(failed_groups)}"
                    )
                    self._emit_progress(
                        "parsing_group_pages",
                        total_groups=total_groups,
                        completed_groups=completed,
                        successful_groups=len(group_docs),
                        failed_groups=len(failed_groups),
                        progress_percent=int(completed / total_groups * 100),
                    )

        success_rate = len(group_docs) / total_groups if total_groups else 0
        if success_rate < self.min_success_rate:
            logger.warning(
                f"Low ISTU API success rate: {len(group_docs)}/{total_groups} "
                f"(threshold={self.min_success_rate}). Saving available data anyway."
            )
        if failed_groups:
            logger.warning(f"Failed groups count: {len(failed_groups)}. Examples: {', '.join(failed_groups[:10])}")

        docs_by_name = {doc["group"]: doc for doc in group_docs}
        for group in groups:
            docs_by_name.setdefault(group["name"], {"group": group["name"], "schedule": []})
        group_docs = sorted(docs_by_name.values(), key=lambda item: item["group"])
        empty_schedule_groups = [doc["group"] for doc in group_docs if not doc["schedule"]]

        self._emit_progress(
            "building_derived_schedules",
            total_groups=total_groups,
            successful_groups=len(group_docs) - len(failed_groups),
            failed_groups=len(failed_groups),
            empty_schedule_groups=len(empty_schedule_groups),
        )
        teacher_docs, auditory_docs, prepods = build_teacher_and_auditory_schedules(all_events)

        logger.info(
            f"ISTU API loading completed: institutes={len(institutes)}, groups={len(groups)}, "
            f"teachers={len(teacher_docs)}, auditories={len(auditory_docs)}, empty={len(empty_schedule_groups)}"
        )
        self._emit_progress(
            "completed",
            institutes_total=len(institutes),
            total_groups=total_groups,
            successful_groups=len(group_docs) - len(failed_groups),
            failed_groups=len(failed_groups),
            empty_schedule_groups=len(empty_schedule_groups),
            group_schedules=len(group_docs),
            teacher_schedules=len(teacher_docs),
            auditories_schedules=len(auditory_docs),
        )

        return {
            "institutes": institutes,
            "courses": courses,
            "groups": [{"name": g["name"], "course": g["course"], "institute": g["institute"]} for g in groups],
            "schedule": group_docs,
            "prepods": prepods,
            "prepods_schedule": teacher_docs,
            "auditories_schedule": auditory_docs,
        }
