import locale
import platform
import time
from datetime import datetime, timedelta
import pytz

from functions.dated_lessons import DAY_ORDER, lessons_on_date, transfer_note, week_dates
from functions.find_week import find_week

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')
# определяем на Linux или на Windows мы запускаемся
locale_name = ('ru_RU.UTF-8' if platform.system() == 'Linux' else 'ru_RU')
locale.setlocale(locale.LC_TIME, locale_name)


def _week_for_tomorrow(week: str) -> str:
    """Чётность недели, в которую попадает завтрашний день.

    ``week`` — чётность текущей недели (из find_week). Если сегодня воскресенье,
    то завтра уже понедельник следующей недели и чётность меняется.
    """
    if week not in ('odd', 'even'):
        return week
    if datetime.now(TZ_IRKUTSK).weekday() != 6:  # 6 — воскресенье
        return week
    return 'even' if week == 'odd' else 'odd'

def day_creating(day):
    day = datetime.strptime(day, "%Y-%m-%d")
    months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября',
              'декабря']
    day_week = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']

    year = int(day.year)
    int_month = int(day.month)
    int_day = int(day.day)

    month = months[int_month - 1]

    today = datetime(int(year), int_month, int_day)

    int_day_week = today.weekday()

    return str(day_week[int_day_week]) + ', ' + str(int_day) + ' ' + str(month) + ' ' + str(year) + ' г.'



def schedule_view_exams(schedule):
    schedule_str = []

    for exam in schedule:
        lessons_str = '-------------------------------------\n'
        day = day_creating(exam['time'].split(' ')[0])
        name = exam['name']
        time = exam['time'].split(' ')[1][:5]
        prep = exam['prep']
        aud = f'Аудитория: {exam["aud"]}\n' if exam["aud"] and exam["aud"][0] else ''

        if time == '00:00':
            lessons_str += f'{aud}' \
                           f'👉{name}\n' \
                           f'{prep}'
        else:
            lessons_str += f'{time}\n' \
                           f'{aud}' \
                           f'👉{name}\n' \
                           f'{prep}'

        lessons_str += '\n-------------------------------------\n'

        # Проверка, что день не пустой
        schedule_str.append(f'\n🍏{day}🍏\n'
                            f'{lessons_str}')
    return schedule_str


def _monday_for_week(week: str):
    """Понедельник недели ``week``: текущей, если чётность совпадает, иначе следующей
    (бот показывает расписание на текущую и следующую неделю)."""
    today = datetime.now(TZ_IRKUTSK).date()
    monday = today - timedelta(days=today.weekday())
    if week in ('odd', 'even') and week != find_week():
        monday += timedelta(days=7)
    return monday


def _lesson_str(lesson: dict, tail: str, show_aud: bool = True) -> str:
    """Одна пара в тексте расписания; tail — преподаватель (для студентов) или группы."""
    name = lesson['name']
    time = lesson['time']
    if name == 'свободно':
        return f'{time}\n{name}'

    info = lesson['info'].replace(",", "")
    aud = ''
    if show_aud and lesson["aud"] and lesson["aud"][0]:
        aud = f'Аудитория: {", ".join(lesson["aud"])}\n'
    text = f'{time}\n' \
           f'{aud}' \
           f'👉{name}\n' \
           f'{info} {tail}'
    note = transfer_note(lesson)
    if note:
        text += f'\n{note}'
    return text


def _stud_tail(lesson: dict) -> str:
    return ', '.join(lesson.get('prep') or [])


def _prep_tail(lesson: dict) -> str:
    return ', '.join(lesson.get('groups') or [])


def _day_in_str(schedule: list, day, week: str, tail, marker: str, aud=None) -> str:
    """Расписание одного дня ``day`` (с учётом переносов) или '' если пар нет."""
    lessons = lessons_on_date(schedule, day, week)
    if not lessons:
        return ''
    lessons_str = '-------------------------------------------\n'
    for lesson in lessons:
        lessons_str += _lesson_str(lesson, tail(lesson), show_aud=not aud)
        # Если выводим расписание аудитории, то добавляем информацию о преподавателе.
        if aud and lesson['name'] != 'свободно' and lesson.get('prep') and lesson['prep'][0]:
            lessons_str += f'\n{", ".join(lesson["prep"])}'
        lessons_str += '\n-------------------------------------------\n'
    day_name = DAY_ORDER[day.weekday()].upper()
    return f'\n{marker}{day_name}{marker}\n{lessons_str}'


def _full_week_in_str(schedule: list, week: str, monday, tail, aud=None) -> list:
    monday = datetime.strptime(monday, '%Y-%m-%d').date() if monday else _monday_for_week(week)
    today = datetime.now(TZ_IRKUTSK).date()
    schedule_str = []
    for day_name, day in week_dates(monday).items():
        marker = '🍏' if day == today else '🍎'
        day_str = _day_in_str(schedule, day, week, tail, marker, aud=aud)
        if day_str:
            schedule_str.append(day_str)
    return schedule_str


def full_schedule_in_str(schedule: list, week: str, monday: str = None) -> list:
    return _full_week_in_str(schedule, week, monday, _stud_tail)


def get_one_day_schedule_in_str(schedule: list, week: str) -> str:
    today = datetime.now(TZ_IRKUTSK).date()
    return _day_in_str(schedule, today, week, _stud_tail, '🍏')


def get_next_day_schedule_in_str(schedule: list, week: str) -> str:
    week = _week_for_tomorrow(week)
    tomorrow = datetime.now(TZ_IRKUTSK).date() + timedelta(days=1)
    return _day_in_str(schedule, tomorrow, week, _stud_tail, '🍎')


# Расписание для преподавателей
def get_one_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    today = datetime.now(TZ_IRKUTSK).date()
    return _day_in_str(schedule, today, week, _prep_tail, '🍏')


def get_next_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    week = _week_for_tomorrow(week)
    tomorrow = datetime.now(TZ_IRKUTSK).date() + timedelta(days=1)
    return _day_in_str(schedule, tomorrow, week, _prep_tail, '🍎')


def full_schedule_in_str_prep(schedule: list, week: str, aud=None, monday: str = None) -> list:
    return _full_week_in_str(schedule, week, monday, _prep_tail, aud=aud)


def get_now_lesson_in_str_stud(now_lessons: list):
    now_lessons_str = ''
    for near_lesson in now_lessons:
        name = near_lesson['name']

        now_lessons_str += '-------------------------------------------\n'
        aud = f'Аудитория: {", ".join(near_lesson["aud"])}\n' if near_lesson["aud"] and near_lesson["aud"][0] else ''

        time = near_lesson['time']
        info = near_lesson['info'].replace(",", "")
        prep = ', '.join(near_lesson['prep'])

        now_lessons_str += f'{time}\n' \
                           f'{aud}' \
                           f'👉{name}\n' \
                           f'{info} {prep}\n'
        note = transfer_note(near_lesson)
        if note:
            now_lessons_str += f'{note}\n'
    now_lessons_str += '-------------------------------------------\n'
    return now_lessons_str


def get_now_lesson_in_str_prep(now_lessons: list):
    now_lessons_str = ''
    for near_lesson in now_lessons:
        name = near_lesson['name']
        now_lessons_str += '-------------------------------------------\n'

        aud = f'Аудитория: {", ".join(near_lesson["aud"])}\n' if near_lesson["aud"] and near_lesson["aud"][0] else ''

        time = near_lesson['time']
        info = near_lesson['info'].replace(",", "")
        groups = ', '.join(near_lesson['groups'])

        now_lessons_str += f'{time}\n' \
                           f'{aud}' \
                           f'👉{name}\n' \
                           f'{info} {groups}\n'
        note = transfer_note(near_lesson)
        if note:
            now_lessons_str += f'{note}\n'
    now_lessons_str += '-------------------------------------------\n'
    return now_lessons_str
