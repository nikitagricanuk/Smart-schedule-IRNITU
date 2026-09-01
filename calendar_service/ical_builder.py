import hashlib
import re
from datetime import datetime, timedelta

import pytz
from icalendar import Calendar, Event

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')

DAY_ORDER = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
LESSON_DURATION_MINUTES = 90

_SUBGROUP_RE = re.compile(r'подгруппа\s*(\d+)', re.IGNORECASE)


def _lesson_subgroup(lesson: dict) -> int:
    """Номер подгруппы из поля info ('( Лаб. раб. подгруппа 2 )'), 0 — для всей группы."""
    match = _SUBGROUP_RE.search(lesson.get('info') or '')
    return int(match.group(1)) if match else 0


def _academic_year_start(now=None):
    now = now or datetime.now(TZ_IRKUTSK)
    # Август уже относим к новому учебному году: иначе в конце августа подписка
    # якорится на прошлый сентябрь и весь год занятий оказывается в прошлом.
    year = now.year if now.month >= 8 else now.year - 1
    return TZ_IRKUTSK.localize(datetime(year, 9, 1))


def week0_monday(now=None):
    """Понедельник недели, в которую попадает 1 сентября текущего учебного года.
    Эта неделя считается ЧЁТНОЙ (см. functions_api/functions/find_week.py)."""
    sep = _academic_year_start(now)
    return sep - timedelta(days=sep.weekday())


def _recurrence_end(now=None):
    """Конец учебного года с запасом, чтобы подписка не накапливала лишние повторения."""
    sep = _academic_year_start(now)
    return TZ_IRKUTSK.localize(datetime(sep.year + 1, 8, 31, 23, 59, 59))


def _first_occurrence_date(monday, day_name: str, week: str):
    if day_name not in DAY_ORDER:
        return None
    first_date = monday + timedelta(days=DAY_ORDER.index(day_name))
    # week0 (неделя с 1 сентября) — чётная, поэтому нечётные пары впервые
    # проходят на неделю позже.
    if week == 'odd':
        first_date += timedelta(days=7)
    return first_date


def _stable_uid(*parts) -> str:
    raw = '|'.join(str(part) for part in parts)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest() + '@smart-schedule-irnitu'


def _build_event(day_name: str, lesson: dict, monday, week_start, horizon_end) -> "Event | None":
    name = lesson.get('name')
    week = lesson.get('week')
    time_str = lesson.get('time')
    if not name or name == 'свободно' or week not in ('odd', 'even', 'all'):
        return None
    if not time_str or ':' not in time_str:
        return None
    try:
        hours, minutes = (int(part) for part in time_str.split(':'))
    except ValueError:
        return None

    first_date = _first_occurrence_date(monday, day_name, 'even' if week == 'all' else week)
    if first_date is None:
        return None

    interval = 1 if week == 'all' else 2
    # Не тащим в подписку прошедшие занятия: сдвигаем первое повторение вперёд
    # целыми шагами (шаг = период повторения, поэтому чётность недели не
    # ломается) до недели, в которой мы сейчас находимся.
    step = timedelta(days=7 * interval)
    while first_date < week_start:
        first_date += step
    if first_date.date() > horizon_end.date():
        return None

    dtstart_local = TZ_IRKUTSK.localize(
        datetime(first_date.year, first_date.month, first_date.day, hours, minutes)
    )
    dtend_local = dtstart_local + timedelta(minutes=LESSON_DURATION_MINUTES)

    aud = ', '.join(item for item in lesson.get('aud', []) if item)
    prep = ', '.join(item for item in lesson.get('prep', []) if item)
    groups = ', '.join(item for item in lesson.get('groups', []) if item)

    summary = name
    if lesson.get('info'):
        summary = f"{name} {lesson['info']}"

    description_lines = []
    if prep:
        description_lines.append(f'Преподаватель: {prep}')
    if groups:
        description_lines.append(f'Группы: {groups}')

    event = Event()
    event.add('summary', summary)
    event.add('dtstart', dtstart_local.astimezone(pytz.utc))
    event.add('dtend', dtend_local.astimezone(pytz.utc))
    event.add('location', aud or 'не указана')
    if description_lines:
        event.add('description', '\n'.join(description_lines))

    event.add('rrule', {
        'freq': 'weekly',
        'interval': interval,
        'until': horizon_end.astimezone(pytz.utc),
    })

    event['uid'] = _stable_uid(day_name, week, time_str, name, aud, prep, groups)
    event.add('dtstamp', datetime.now(pytz.utc))
    return event


def build_calendar(schedule_doc: dict, calendar_name: str, subgroup: int = 0) -> bytes:
    """Строит .ics со всеми занятиями из документа расписания (группы или преподавателя).

    subgroup: 0 — всё расписание; 1/2/3 — только общие занятия и занятия этой подгруппы.
    """
    cal = Calendar()
    cal.add('prodid', '-//Smart Schedule IRNITU//istu-schedule//RU')
    cal.add('version', '2.0')
    cal.add('calscale', 'GREGORIAN')
    cal.add('method', 'PUBLISH')
    cal.add('x-wr-calname', calendar_name)
    cal.add('x-wr-timezone', 'Asia/Irkutsk')

    now = datetime.now(TZ_IRKUTSK)
    monday = week0_monday(now)
    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    horizon_end = _recurrence_end(now)

    for day in schedule_doc.get('schedule') or []:
        day_name = day.get('day')
        for lesson in day.get('lessons') or []:
            if subgroup and _lesson_subgroup(lesson) not in (0, subgroup):
                continue
            event = _build_event(day_name, lesson, monday, week_start, horizon_end)
            if event is not None:
                cal.add_component(event)

    return cal.to_ical()
