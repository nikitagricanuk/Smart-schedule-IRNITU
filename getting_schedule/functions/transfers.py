"""Разовые переносы занятий (``type == "query"`` в API ИРНИТУ).

API публикует перенос отдельной карточкой, например::

    2-ая подгруппа «Основы информационной безопасности», перенос из Ж-312,
    причина: инженерные каникулы

в день/пару/аудиторию, куда занятие переехало. Здесь карточка разбирается и
сопоставляется с исходной регулярной парой группы: если пара нашлась, к ней
дописывается ``transfers`` (на какую дату, время и аудиторию она переехала),
и бот с календарём показывают её на новом месте вместо отдельного занятия.
Если исходную пару найти не удалось, перенос остаётся разовым занятием с
полем ``date``.
"""
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

_SUBGROUP_RE = re.compile(
    r'(\d)\s*-?\s*[а-яё]*\s+подгрупп[а-яё]*|подгрупп[а-яё]*\s*(?:№\s*)?(\d)', re.IGNORECASE
)
_QUOTED_RE = re.compile(r'[«"“]([^»"”]+)[»"”]')
_FROM_DATE_RE = re.compile(
    r'перенос\s+с\s+(?:(\d{4})\.(\d{1,2})\.(\d{1,2})|(\d{1,2})\.(\d{1,2})\.(\d{4}))', re.IGNORECASE
)
_FROM_AUD_RE = re.compile(r'перенос\s+из\s+(?:ауд(?:итории|\.)?\s*)?([^,;]+)', re.IGNORECASE)
_FROM_PARA_RE = re.compile(r'перенос\s+с\s+(\d)\s*-?\s*[а-яё]*\s+пар', re.IGNORECASE)
_REASON_RE = re.compile(r'причин[аы]\s*:?\s*(.+)$', re.IGNORECASE)
_LOOKS_LIKE_TRANSFER_RE = re.compile(r'перенос\s+(?:из|с)\s|причина\s*:', re.IGNORECASE)
# Приказ о переносе (query type 1): "<причина> | перевод ИСТб-24-2-2, Гордин А.С. в В-104".
_ORDER_TARGET_AUD_RE = re.compile(r'\sв\s+(\S+)\s*$')

# Типы записей в queries[] API: 0 — проектная деятельность (просто разовое занятие),
# 1 — приказ о переносе (там новая аудитория), 2 — сам перенос, как его видит группа.
QUERY_TYPE_ORDER = 1
QUERY_TYPE_TRANSFER = 2
_LESSON_SUBGROUP_RE = re.compile(r'подгруппа\s*(\d+)', re.IGNORECASE)

DAY_NAMES = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']


def normalize_name(value: str) -> str:
    value = (value or '').lower().replace('ё', 'е')
    value = re.sub(r'[«»"“”]', '', value)
    return ' '.join(value.split()).strip(' .,')


def lesson_subgroup(info: str) -> Optional[int]:
    match = _LESSON_SUBGROUP_RE.search(info or '')
    return int(match.group(1)) if match else None


def _parse_date(groups) -> Optional[date]:
    y1, m1, d1, d2, m2, y2 = groups
    try:
        if y1:
            return date(int(y1), int(m1), int(d1))
        return date(int(y2), int(m2), int(d2))
    except (TypeError, ValueError):
        return None


def parse_transfer_title(title: str) -> Dict[str, Any]:
    """Разбирает текст карточки переноса.

    Возвращает subject (название предмета), subgroup (номер или None),
    from_aud, from_date (date или None) и reason.
    """
    title = ' '.join((title or '').split())

    subgroup_match = _SUBGROUP_RE.search(title)
    subgroup = int(subgroup_match.group(1) or subgroup_match.group(2)) if subgroup_match else None

    quoted = _QUOTED_RE.search(title)
    if quoted:
        subject = quoted.group(1)
    else:
        subject = re.split(r',|\s+перенос\b', title, maxsplit=1, flags=re.IGNORECASE)[0]
        if subgroup_match:
            subject = subject.replace(subgroup_match.group(0), '')
    subject = ' '.join(subject.split()).strip(' .,')

    from_date_match = _FROM_DATE_RE.search(title)
    from_date = _parse_date(from_date_match.groups()) if from_date_match else None

    from_aud = None
    if not from_date_match:
        aud_match = _FROM_AUD_RE.search(title)
        if aud_match:
            from_aud = aud_match.group(1).strip(' .')

    from_para_match = _FROM_PARA_RE.search(title)
    from_para = int(from_para_match.group(1)) if from_para_match else None

    reason_match = _REASON_RE.search(title)
    reason = reason_match.group(1).strip(' .') if reason_match else None

    return {
        'subject': subject,
        'subgroup': subgroup,
        'from_aud': from_aud or None,
        'from_date': from_date,
        'from_para': from_para,
        'reason': reason or None,
    }


def is_transfer(query: Optional[Dict[str, Any]], title: str) -> bool:
    """Карточка — перенос регулярной пары, а не разовое занятие (проектная деятельность)."""
    if query and query.get('type') is not None:
        return query.get('type') == QUERY_TYPE_TRANSFER
    return bool(_LOOKS_LIKE_TRANSFER_RE.search(title or ''))


def transfer_target_auditories(
    item: Dict[str, Any],
    query: Optional[Dict[str, Any]],
    auditory_names: Dict[int, str],
    from_aud: Optional[str] = None,
) -> List[str]:
    """Аудитория, куда переехала пара.

    Проверено на данных API (28.10.2026, ИСТб-24-2): auditories_ids самой карточки
    переноса — новая аудитория (474 = В-104), а auds связанного приказа (query
    type 1) — старая (825 = Ж-312), хотя текст приказа кончается "... в В-104".
    Поэтому сначала берём аудиторию карточки, затем текст приказа, затем его auds,
    и никогда не возвращаем аудиторию из "перенос из ...".
    """
    old = normalize_name(from_aud) if from_aud else None

    def usable(names):
        return [name for name in names if name and normalize_name(name) != old]

    card = usable([auditory_names[aud_id] for aud_id in item.get('auditories_ids') or [] if aud_id in auditory_names])
    if card:
        return card

    orders = [related for related in (query or {}).get('related_queries') or []
              if related.get('type') == QUERY_TYPE_ORDER]
    for related in orders:
        match = _ORDER_TARGET_AUD_RE.search(' '.join((related.get('title') or '').split()))
        if match:
            by_text = usable([match.group(1).strip(' .,')])
            if by_text:
                return by_text
    for related in orders:
        by_id = usable([auditory_names[aud_id] for aud_id in related.get('auds') or [] if aud_id in auditory_names])
        if by_id:
            return by_id
    return []


def week_parity_of_date(day: date) -> str:
    """Чётность недели даты: неделя с 1 сентября чётная (см. find_week в functions_api)."""
    sep = date(day.year if day.month >= 9 else day.year - 1, 9, 1)
    week0 = sep.toordinal() - sep.weekday()
    monday = day.toordinal() - day.weekday()
    return 'odd' if ((monday - week0) // 7) % 2 else 'even'


def _time_key(value: str) -> int:
    try:
        hours, minutes = value.split(':')
        return int(hours) * 60 + int(minutes)
    except (AttributeError, ValueError):
        return -1


def find_original_lesson(
    day_to_lessons: Dict[str, List[Dict[str, Any]]],
    parsed: Dict[str, Any],
    original_date: date,
    target_time: str,
    original_time: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Ищет регулярную пару, к которой относится перенос.

    Пара должна стоять в день недели и неделю исходной даты, иметь то же
    название и (если в переносе указана) ту же подгруппу. Если подходящих
    несколько, берём ту, что была в аудитории «перенос из ...», затем — в то
    же время. Неоднозначный результат — None: лучше показать перенос отдельной
    карточкой, чем передвинуть чужую пару.
    """
    subject = normalize_name(parsed.get('subject'))
    if not subject:
        return None
    day_name = DAY_NAMES[original_date.weekday()]
    parity = week_parity_of_date(original_date)

    candidates = []
    for lesson in day_to_lessons.get(day_name, []):
        if lesson.get('date') or lesson.get('week') not in ('all', parity):
            continue
        name = normalize_name(lesson.get('name'))
        if name != subject and subject not in name and name not in subject:
            continue
        candidates.append(lesson)

    if original_time:
        candidates = [item for item in candidates if _time_key(item.get('time')) == _time_key(original_time)]
    if parsed.get('subgroup') is not None:
        candidates = [item for item in candidates if lesson_subgroup(item.get('info')) == parsed['subgroup']]

    if len(candidates) > 1:
        exact = [item for item in candidates if normalize_name(item.get('name')) == subject]
        candidates = exact or candidates
    if len(candidates) > 1 and parsed.get('from_aud'):
        from_aud = normalize_name(parsed['from_aud'])
        by_aud = [item for item in candidates if from_aud in [normalize_name(aud) for aud in item.get('aud') or []]]
        candidates = by_aud or candidates
    if len(candidates) > 1:
        same_time = [item for item in candidates if _time_key(item.get('time')) == _time_key(target_time)]
        candidates = same_time or candidates

    return candidates[0] if len(candidates) == 1 else None


def make_transfer_record(
    original_date: date,
    target_date: date,
    target_time: str,
    target_aud: List[str],
    parsed: Dict[str, Any],
) -> Dict[str, Any]:
    record = {
        'date': original_date.isoformat(),
        'to_date': target_date.isoformat(),
        'to_time': target_time,
        'to_aud': target_aud,
    }
    if parsed.get('reason'):
        record['reason'] = parsed['reason']
    return record


def add_transfer(lesson: Dict[str, Any], record: Dict[str, Any]) -> None:
    transfers = lesson.setdefault('transfers', [])
    if record not in transfers:
        transfers.append(record)
        transfers.sort(key=lambda item: (item['date'], item['to_date'], _time_key(item['to_time'])))


def parse_iso_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None
