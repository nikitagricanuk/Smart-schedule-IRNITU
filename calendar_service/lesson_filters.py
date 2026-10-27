"""Пользовательские правила скрытия занятий.

Правила лежат в документе пользователя (коллекция users):
    hidden_lesson_types — виды занятий, скрытые целиком ('Проект', 'Лекция', ...);
    hidden_lessons      — отдельные предметы, ключ 'название|вид' (см. lesson_key).

ВАЖНО: файл продублирован в calendar_service/lesson_filters.py и
tg_bot/tools/lesson_filters.py (у сервисов разные Docker-контексты).
Меняйте обе копии — тест test_lesson_filters следит, чтобы они совпадали.
"""

import re

_SUBGROUP_RE = re.compile(r'подгруппа\s*\d+', re.IGNORECASE)


def lesson_type(lesson: dict) -> str:
    """Вид занятия из поля info: '( Лаб. раб. подгруппа 2 )' -> 'Лаб. раб.', '( Проект )' -> 'Проект'."""
    info = _SUBGROUP_RE.sub('', lesson.get('info') or '')
    return info.strip().strip('()').strip()


def lesson_key(lesson: dict) -> str:
    """Ключ предмета без привязки к аудитории/времени: смена аудитории или перенос
    пары не должны возвращать скрытое занятие в расписание."""
    return f"{lesson.get('name') or ''}|{lesson_type(lesson)}"


def _is_transfer(name: str) -> bool:
    return 'перенос' in name.lower()


def user_rules(user) -> tuple:
    """(скрытые виды, скрытые предметы) из документа пользователя."""
    if not user:
        return set(), set()
    return set(user.get('hidden_lesson_types') or []), set(user.get('hidden_lessons') or [])


def is_hidden(lesson: dict, hidden_types: set, hidden_lessons: set) -> bool:
    ltype = lesson_type(lesson)
    if ltype and ltype in hidden_types:
        return True
    if not hidden_lessons:
        return False
    name = lesson.get('name') or ''
    if lesson_key(lesson) in hidden_lessons:
        return True
    # Разовые переносы называются иначе ('Разовый перенос «Предмет», на ...'),
    # но относятся к тому же предмету — скрываем и их.
    if _is_transfer(name):
        for key in hidden_lessons:
            hidden_name, _, hidden_type = key.rpartition('|')
            if hidden_name and hidden_type == ltype and hidden_name in name:
                return True
    return False


def filter_schedule(schedule: list, user) -> list:
    """Копия списка дней расписания без скрытых пользователем занятий."""
    hidden_types, hidden_lessons = user_rules(user)
    if not hidden_types and not hidden_lessons:
        return schedule
    filtered = []
    for day in schedule or []:
        lessons = [lesson for lesson in day.get('lessons') or []
                   if not is_hidden(lesson, hidden_types, hidden_lessons)]
        filtered.append({**day, 'lessons': lessons})
    return filtered


def _visible_lessons(schedule: list):
    for day in schedule or []:
        for lesson in day.get('lessons') or []:
            name = lesson.get('name') or ''
            if name and name != 'свободно':
                yield lesson


def type_options(schedule: list) -> list:
    """Виды занятий, которые встречаются в расписании, по алфавиту."""
    return sorted({lesson_type(lesson) for lesson in _visible_lessons(schedule)} - {''})


def lesson_options(schedule: list) -> list:
    """Ключи предметов из расписания (без разовых переносов), по алфавиту."""
    return sorted({lesson_key(lesson) for lesson in _visible_lessons(schedule)
                   if not _is_transfer(lesson.get('name') or '')})


def lesson_label(key: str) -> str:
    name, _, ltype = key.rpartition('|')
    return f'{name} ({ltype})' if ltype else name
