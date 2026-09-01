from datetime import datetime, timedelta

DEBUG = False


def find_week():
    """Определение чётности текущей недели.

    Счёт непрерывный от недели с 1 сентября (она чётная), см.
    functions_api/functions/find_week.py.
    """
    now = datetime.now()
    sep = datetime(now.year if now.month >= 9 else now.year - 1, 9, 1)
    d1 = sep - timedelta(days=sep.weekday())
    d2 = now - timedelta(days=now.weekday())

    parity = ((d2 - d1).days // 7) % 2
    return 'odd' if parity else 'even'


def forming_user_to_submit(
        chat_id: int,
        group: str,
        notifications: int,
        day_now: str,
        time_now: datetime,
        week: str) -> dict:
    """Формирование информации о пользователе для отправки"""

    # определяем фактическое время пары (прибавляем к текущему времени время напоминания)
    lesson_time = (time_now + timedelta(minutes=notifications)).strftime('%H:%M')

    user = {
        'chat_id': chat_id,
        'group': group,
        'week': week,
        'day': day_now,
        'notifications': notifications,
        'time': lesson_time
    }

    return user


def check_that_user_has_reminder_enabled_for_the_current_time(time_now, user_day_reminder_time: list) -> bool:
    """Проверка, что у пользователя включено  напоминание на текущее время"""
    if DEBUG:
        return True
    hours_now = int(time_now.strftime('%H'))
    minutes_now = time_now.strftime('%M')

    return user_day_reminder_time and f'{hours_now}:{minutes_now}' in user_day_reminder_time


def get_schedule_from_right_day(schedule, day_now) -> list:
    """Получение расписания из нужного дня"""
    for day in schedule:
        # находим нужный день
        if day['day'] == day_now:
            lessons = day['lessons']
            return lessons


def check_that_the_lesson_has_the_right_time(time, lesson_time, lesson, week) -> bool:
    """Проверка, что урок имеет нужное время и неделю"""
    if DEBUG:
        return True
    return time in lesson_time and (lesson['week'] == week or lesson['week'] == 'all')


def forming_message_text(lessons, week, time):
    """Формирование текста для сообщения"""
    lessons_for_reminders = ''

    count = 0
    for lesson in lessons:
        lesson_time = lesson['time']
        # находим нужные пары (в нужное время)
        if check_that_the_lesson_has_the_right_time(time, lesson_time, lesson, week):
            name = lesson['name']
            # пропускаем свободные дни
            if name == 'свободно':
                continue

            # формируем сообщение
            lessons_for_reminders += '-------------------------------------------\n'
            aud = lesson['aud']
            if aud:
                aud = f'Аудитория: {",".join(aud)}\n'
            time = lesson['time']
            info = lesson['info']
            prep = lesson['prep']

            lessons_for_reminders += f'Начало в {time}\n' \
                                     f'{aud}' \
                                     f'{name}\n' \
                                     f'{info} {",".join(prep)}\n'
            count += 1

    if count > 0:
        lessons_for_reminders += '-------------------------------------------\n'

    return lessons_for_reminders
