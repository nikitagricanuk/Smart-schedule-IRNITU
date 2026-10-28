"""Пары на конкретную дату с учётом разовых переносов.

Расписание в базе — по дням недели и чётности. Поверх него лежат:
  * ``lesson['transfers']`` — разовые переносы регулярной пары:
    ``{'date': исходная дата, 'to_date', 'to_time', 'to_aud', 'reason'}``;
  * ``lesson['date']`` — разовое занятие только на эту дату (перенос, для
    которого не нашлась исходная пара). Старые записи без поля ``date``
    узнаём по названию "Разовый перенос ..., на YYYY.MM.DD перенос".

ВНИМАНИЕ: файл существует в двух одинаковых копиях —
functions_api/functions/dated_lessons.py и notification_service/tools/dated_lessons.py.
Меняйте обе (тест notification_service/tests/test_dated_lessons.py это проверяет).
"""
import re
from datetime import date, timedelta

DAY_ORDER = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']

_LEGACY_TRANSFER_DATE_RE = re.compile(r'\bна\s+(\d{4})\.(\d{2})\.(\d{2})')


def _time_key(value) -> int:
    try:
        hours, minutes = str(value).split(':')
        return int(hours) * 60 + int(minutes)
    except ValueError:
        return 0


def one_off_date(lesson: dict):
    """ISO-дата разового занятия или None для регулярной пары."""
    if lesson.get('date'):
        return lesson['date']
    name = lesson.get('name') or ''
    if name.lower().startswith('разовый перенос'):
        match = _LEGACY_TRANSFER_DATE_RE.search(name)
        if match:
            return '-'.join(match.groups())
    return None


def short_date(iso_date: str) -> str:
    year, month, day = iso_date.split('-')
    return f'{day}.{month}'


def transfer_note(lesson: dict) -> str:
    """Строка о переносе для показа под парой ('' если переноса нет)."""
    moved_in = lesson.get('moved_in')
    moved_away = lesson.get('moved_away')
    reason = None
    if moved_away:
        reason = moved_away.get('reason')
        aud = ', '.join(item for item in moved_away.get('to_aud') or [] if item)
        where = f'{short_date(moved_away["to_date"])} в {moved_away["to_time"]}'
        note = f'🚫 Пары не будет: перенесена на {where}' + (f', ауд. {aud}' if aud else '')
    elif moved_in:
        reason = moved_in.get('reason')
        parts = []
        if moved_in['date'] != moved_in['to_date']:
            parts.append(f'с {short_date(moved_in["date"])}')
        if moved_in.get('from_time') and moved_in['from_time'] != moved_in['to_time']:
            parts.append(f'время {moved_in["from_time"]} → {moved_in["to_time"]}')
        from_aud = ', '.join(item for item in moved_in.get('from_aud') or [] if item)
        to_aud = ', '.join(item for item in moved_in.get('to_aud') or [] if item)
        if from_aud and to_aud and from_aud != to_aud:
            parts.append(f'ауд. {from_aud} → {to_aud}')
        note = '🔄 Разовый перенос' + (': ' + ', '.join(parts) if parts else '')
    else:
        return ''
    if reason:
        note += f' ({reason})'
    return note


def lessons_on_date(schedule: list, day: date, week: str = None) -> list:
    """Пары одного дня ``day`` с применёнными переносами, отсортированные по времени.

    Перенесённая на этот день пара возвращается копией с новыми временем и
    аудиторией и ключом ``moved_in``; пара, перенесённая с этого дня на другой,
    — копией с ключом ``moved_away``. ``week`` — чётность недели этого дня.
    """
    iso = day.isoformat()
    day_name = DAY_ORDER[day.weekday()]
    result = []
    for one_day in schedule:
        for lesson in one_day.get('lessons') or []:
            dated = one_off_date(lesson)
            if dated is not None:
                if dated == iso:
                    result.append(lesson)
                continue

            transfers = lesson.get('transfers') or []
            for record in transfers:
                if record.get('to_date') == iso:
                    moved = dict(lesson)
                    moved['time'] = record.get('to_time') or lesson['time']
                    moved['aud'] = record.get('to_aud') or lesson.get('aud')
                    moved['moved_in'] = dict(record, from_time=lesson['time'], from_aud=lesson.get('aud'))
                    # Копия уже привязана к дате, чётность исходной пары к ней не относится.
                    moved['week'] = 'all'
                    result.append(moved)

            if one_day.get('day') != day_name:
                continue
            if week and lesson.get('week') not in (week, 'all'):
                continue
            moved_from_here = [record for record in transfers if record.get('date') == iso]
            if not moved_from_here:
                result.append(lesson)
            elif all(record.get('to_date') != iso for record in moved_from_here):
                stub = dict(lesson)
                stub['moved_away'] = moved_from_here[0]
                result.append(stub)

    result.sort(key=lambda item: _time_key(item.get('time')))
    return result


def week_dates(monday: date) -> dict:
    """{название дня: дата} для недели, начинающейся с ``monday``."""
    return {name: monday + timedelta(days=index) for index, name in enumerate(DAY_ORDER)}
