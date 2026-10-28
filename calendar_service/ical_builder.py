import hashlib
import re
from datetime import datetime, timedelta

import pytz
from icalendar import Calendar, Event

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')

DAY_ORDER = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
LESSON_DURATION_MINUTES = 90

_SUBGROUP_RE = re.compile(r'подгруппа\s*(\d+)', re.IGNORECASE)
_TRANSFER_DATE_RE = re.compile(r'\bна\s+(\d{4})\.(\d{2})\.(\d{2})')
_TRANSFER_FROM_DATE_RE = re.compile(r'перенос\s+с\s+(\d{4})\.(\d{2})\.(\d{2})', re.IGNORECASE)


def _is_one_time_transfer(name: str) -> bool:
    """Одноразовое занятие (не часть регулярного расписания). Два варианта карточек ISTU:
    'Разовый перенос ..., на YYYY.MM.DD' — заглушка на старом месте, указывает вперёд;
    '..., перенос с YYYY.MM.DD' — сама карточка на новом месте, дата в названии старая.
    Не путать с 'перенос из <ауд>' — это смена аудитории в тот же день, занятие обычное."""
    lowered = name.lower()
    if lowered.startswith('разовый перенос'):
        return True
    if 'перенос' not in lowered:
        return False
    return bool(_TRANSFER_DATE_RE.search(name) or _TRANSFER_FROM_DATE_RE.search(name))


def _parse_transfer_date(name: str):
    """Достаём дату из 'Разовый перенос ..., на 2026.09.01 перенос'."""
    match = _TRANSFER_DATE_RE.search(name)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    try:
        return datetime(year, month, day).date()
    except ValueError:
        return None


def _parse_iso_date(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None


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


ROLLING_RECURRENCE_DAYS = 35


def _recurrence_end(now=None):
    """Скользящий горизонт повторения — не весь учебный год, а несколько недель вперёд.

    Сайт ИРНИТУ отдаёт срез только на одну неделю за раз и размечает классом
    week-odd/week-even/week-all как по-настоящему регулярные пары, так и
    разовые события (например, «Встреча с дирекцией института»), которые
    просто попали в эту неделю — отличить их по разметке нельзя. Раз
    регулярность пары подтверждается только следующим опросом сайта
    (см. GETTING_SCHEDULE_TIME_HOURS), обещать подписке повторения на весь
    год вперёд нельзя: разовое событие «зависает» в календаре на месяцы.
    Короткое окно продлевается само собой при каждой генерации .ics по
    свежим данным, а разовое событие само выпадет из выдачи, как только
    исчезнет с сайта.
    """
    now = now or datetime.now(TZ_IRKUTSK)
    horizon = now + timedelta(days=ROLLING_RECURRENCE_DAYS)
    return horizon.replace(hour=23, minute=59, second=59, microsecond=0)


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

    # Новый формат: разовое занятие несёт дату в поле date (см. getting_schedule).
    lesson_date = _parse_iso_date(lesson.get('date'))
    one_time = lesson_date is not None or _is_one_time_transfer(name)
    # 'на YYYY.MM.DD' указывает вперёд — это дата, куда занятие перенесено,
    # берём её напрямую вместо дня недели/чётности. Другой одноразовый вариант,
    # '... перенос с YYYY.MM.DD' без 'на DATE', — это уже сама перенесённая
    # карточка, стоящая на своём новом дне/времени; дата в названии старая
    # (откуда перенесли) и для вычисления occurrence не годится, поэтому для
    # неё (как и для обычных занятий) считаем дату по дню недели и чётности.
    transfer_date = lesson_date or (_parse_transfer_date(name) if one_time else None)
    if transfer_date is not None:
        if transfer_date < week_start.date():
            return None
        first_date = TZ_IRKUTSK.localize(datetime(transfer_date.year, transfer_date.month, transfer_date.day))
    else:
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

    if one_time:
        event['uid'] = _stable_uid('one-time', first_date.date().isoformat(), time_str, name, aud, prep, groups)
    else:
        event.add('rrule', {
            'freq': 'weekly',
            'interval': interval,
            'until': horizon_end.astimezone(pytz.utc),
        })
        event['uid'] = _stable_uid(day_name, week, time_str, name, aud, prep, groups)

    event.add('dtstamp', datetime.now(pytz.utc))
    return event


def _aud_str(aud) -> str:
    return ', '.join(item for item in aud or [] if item)


def _transfer_events(lesson: dict, series: Event, week_start, horizon_end) -> list:
    """Разовые переносы регулярной пары (lesson['transfers']).

    Перенесённое занятие — это изменённое повторение той же серии (тот же UID и
    RECURRENCE-ID исходного занятия), поэтому календарь показывает пару на новом
    месте/времени вместо исходной, а не лишним событием рядом с ней.
    """
    events = []
    if 'rrule' not in series:
        return events
    series_start = series['dtstart'].dt.astimezone(TZ_IRKUTSK)
    interval = series['rrule'].get('INTERVAL', 1)
    interval_days = 7 * int(interval[0] if isinstance(interval, (list, tuple)) else interval)
    location = str(series.get('location', ''))
    description = str(series.get('description', '')) if series.get('description') else ''

    for record in lesson.get('transfers') or []:
        original_date = _parse_iso_date(record.get('date'))
        target_date = _parse_iso_date(record.get('to_date'))
        if original_date is None or target_date is None:
            continue
        original = TZ_IRKUTSK.localize(datetime(
            original_date.year, original_date.month, original_date.day, series_start.hour, series_start.minute))
        in_series = (
            series_start <= original <= horizon_end
            and (original.date() - series_start.date()).days % interval_days == 0
        )
        if not in_series and target_date < week_start.date():
            continue
        try:
            hours, minutes = (int(part) for part in (record.get('to_time') or '').split(':'))
        except ValueError:
            hours, minutes = series_start.hour, series_start.minute
        start = TZ_IRKUTSK.localize(datetime(target_date.year, target_date.month, target_date.day, hours, minutes))

        new_location = _aud_str(record.get('to_aud')) or location
        note = 'Разовый перенос'
        if original_date != target_date or (hours, minutes) != (series_start.hour, series_start.minute):
            note += f" с {original.strftime('%d.%m %H:%M')}"
        if new_location != location:
            note += f', аудитория {location} → {new_location}'
        if record.get('reason'):
            note += f" (причина: {record['reason']})"

        event = Event()
        event.add('summary', f"🔄 {series['summary']}")
        event.add('dtstart', start.astimezone(pytz.utc))
        event.add('dtend', (start + timedelta(minutes=LESSON_DURATION_MINUTES)).astimezone(pytz.utc))
        event.add('location', new_location)
        event.add('description', '\n'.join(line for line in (note, description) if line))
        if in_series:
            event['uid'] = series['uid']
            event.add('recurrence-id', original.astimezone(pytz.utc))
        else:
            # Исходное занятие вне окна подписки — показываем перенос отдельным событием.
            event['uid'] = _stable_uid('transfer', series['uid'], record.get('date'), record.get('to_date'))
        event.add('dtstamp', datetime.now(pytz.utc))
        events.append(event)
    return events


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
                for override in _transfer_events(lesson, event, week_start, horizon_end):
                    cal.add_component(override)

    return cal.to_ical()
