import hashlib
from datetime import datetime, timedelta

import pytz
from icalendar import Calendar, Event

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')

DAY_ORDER = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
LESSON_DURATION_MINUTES = 90


def _academic_year_start(now=None):
    now = now or datetime.now(TZ_IRKUTSK)
    year = now.year if now.month >= 9 else now.year - 1
    return TZ_IRKUTSK.localize(datetime(year, 9, 1))


def week0_monday(now=None):
    """Понедельник недели, в которую попадает 1 сентября текущего учебного года.
    Эта неделя считается нечётной (см. functions_api/functions/find_week.py)."""
    sep = _academic_year_start(now)
    return sep - timedelta(days=sep.weekday())


def _recurrence_end(monday):
    """Конец учебного года с запасом, чтобы подписка не накапливала лишние повторения."""
    return TZ_IRKUTSK.localize(datetime(monday.year + 1, 8, 31, 23, 59, 59))


def _first_occurrence_date(monday, day_name: str, week: str):
    if day_name not in DAY_ORDER:
        return None
    first_date = monday + timedelta(days=DAY_ORDER.index(day_name))
    if week == 'even':
        first_date += timedelta(days=7)
    return first_date


def _stable_uid(*parts) -> str:
    raw = '|'.join(str(part) for part in parts)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest() + '@smart-schedule-irnitu'


def _build_event(day_name: str, lesson: dict, monday) -> "Event | None":
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

    first_date = _first_occurrence_date(monday, day_name, 'odd' if week == 'all' else week)
    if first_date is None:
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

    interval = 1 if week == 'all' else 2
    event.add('rrule', {
        'freq': 'weekly',
        'interval': interval,
        'until': _recurrence_end(monday).astimezone(pytz.utc),
    })

    event['uid'] = _stable_uid(day_name, week, time_str, name, aud, prep, groups)
    event.add('dtstamp', datetime.now(pytz.utc))
    return event


def build_calendar(schedule_doc: dict, calendar_name: str) -> bytes:
    """Строит .ics со всеми занятиями из документа расписания (группы или преподавателя)."""
    cal = Calendar()
    cal.add('prodid', '-//Smart Schedule IRNITU//istu-schedule//RU')
    cal.add('version', '2.0')
    cal.add('calscale', 'GREGORIAN')
    cal.add('method', 'PUBLISH')
    cal.add('x-wr-calname', calendar_name)
    cal.add('x-wr-timezone', 'Asia/Irkutsk')

    monday = week0_monday()

    for day in schedule_doc.get('schedule') or []:
        day_name = day.get('day')
        for lesson in day.get('lessons') or []:
            event = _build_event(day_name, lesson, monday)
            if event is not None:
                cal.add_component(event)

    return cal.to_ical()
