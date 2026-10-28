import locale
import platform
from datetime import datetime

import pytz

from functions.dated_lessons import lessons_on_date

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')
locale_name = ('ru_RU.UTF-8' if platform.system() == 'Linux' else 'ru_RU')
locale.setlocale(locale.LC_TIME, locale_name)


def get_near_lesson(schedule: list, week: str) -> list:
    """Возвращает ближайшую пару"""

    now = datetime.now(TZ_IRKUTSK)
    hours_now = now.hour
    minutes_now = now.minute

    # пары сегодняшнего дня с учётом разовых переносов; отменённые (перенесённые
    # на другой день) не считаем
    lessons = [lesson for lesson in lessons_on_date(schedule, now.date(), week) if not lesson.get('moved_away')]
    if not lessons:
        return []

    near_lessons = []
    for lesson in lessons:
        hours_lesson, minutes_lesson = map(int, lesson['time'].split(':'))
        # находим ближайшую пару
        if (hours_lesson * 60 + minutes_lesson) - (hours_now * 60 + minutes_now) >= 0 and not near_lessons:
            near_lessons.append(lesson)
        # если несколько пар в одно время (у разных подгрупп)
        elif near_lessons:
            if lesson['time'] == near_lessons[0]['time']:
                near_lessons.append(lesson)

    return near_lessons


def get_now_lesson(schedule: list, week: str) -> list:
    """"Возвращает текущую пару"""

    now = datetime.now(TZ_IRKUTSK)
    hours_now = now.hour
    minutes_now = now.minute

    # пары сегодняшнего дня с учётом разовых переносов; отменённые (перенесённые
    # на другой день) не считаем
    lessons = [lesson for lesson in lessons_on_date(schedule, now.date(), week) if not lesson.get('moved_away')]
    if not lessons:
        return []

    now_lessons = []
    for lesson in lessons:
        hours_lesson, minutes_lesson = map(int, lesson['time'].split(':'))
        # находим ближайшую пару
        a = hours_lesson * 60 + minutes_lesson
        b = hours_now * 60 + minutes_now
        if 0 <= b - a <= 90 and not now_lessons:
            now_lessons.append(lesson)
        # если несколько пар в одно время (у разных подгрупп)
        elif now_lessons:
            if lesson['time'] == now_lessons[0]['time']:
                now_lessons.append(lesson)

    return now_lessons
