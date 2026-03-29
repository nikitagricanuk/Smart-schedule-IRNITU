import os

import requests

from tools.logger import logger

FUNCTIONS_API_URL = os.environ.get('FUNCTIONS_API_URL')
FUNCTIONS_API_CONNECT_TIMEOUT = float(os.environ.get('FUNCTIONS_API_CONNECT_TIMEOUT', '5'))
FUNCTIONS_API_READ_TIMEOUT = float(os.environ.get('FUNCTIONS_API_READ_TIMEOUT', '20'))
FUNCTIONS_API_TIMEOUT = (FUNCTIONS_API_CONNECT_TIMEOUT, FUNCTIONS_API_READ_TIMEOUT)


def get_api_data(url: str, data: dict = None):
    payload = data or {}
    request_url = f'{FUNCTIONS_API_URL}{url}'
    try:
        answer = requests.get(url=request_url, json=payload, timeout=FUNCTIONS_API_TIMEOUT)
        answer.raise_for_status()
        json_answer = answer.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f'functions_api request failed: url={request_url}, error={e}')
        error = APIError(error_msg=e)
        return error
    return json_answer


def find_week():
    url = 'find_week/'
    week = get_api_data(url=url)

    return week

def schedule_view_exams(schedule: list) -> list:
    url = 'creating_schedule/schedule_view_exams/'
    data = {
        'schedule': schedule['exams']['exams']
    }
    schedule_str = get_api_data(url=url, data=data)
    return schedule_str

def full_schedule_in_str(schedule: list, week: str) -> list:
    url = 'creating_schedule/full_schedule_in_str/'
    data = {
        'schedule': schedule,
        'week': week
    }
    schedule_str = get_api_data(url=url, data=data)
    return schedule_str


def get_one_day_schedule_in_str(schedule: list, week: str) -> str:
    url = 'creating_schedule/get_one_day_schedule_in_str/'
    data = {
        'schedule': schedule,
        'week': week
    }
    one_day = get_api_data(url=url, data=data)

    return one_day


def get_next_day_schedule_in_str(schedule: list, week: str) -> str:
    url = 'creating_schedule/get_next_day_schedule_in_str/'
    data = {
        'schedule': schedule,
        'week': week
    }
    next_day = get_api_data(url=url, data=data)
    return next_day


# Расписание для преподавателей
def get_one_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    url = 'creating_schedule/get_one_day_schedule_in_str_prep/'
    data = {
        'schedule': schedule,
        'week': week
    }
    one_day = get_api_data(url=url, data=data)
    return one_day


def get_next_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    url = 'creating_schedule/get_next_day_schedule_in_str_prep/'
    data = {
        'schedule': schedule,
        'week': week
    }
    next_day = get_api_data(url=url, data=data)
    return next_day


def full_schedule_in_str_prep(schedule: list, week: str, aud=None) -> list:
    url = 'creating_schedule/full_schedule_in_str_prep/'
    data = {
        'schedule': schedule,
        'week': week,
        'aud': aud
    }
    schedule_str = get_api_data(url=url, data=data)
    return schedule_str


def get_near_lesson(schedule: list, week: str) -> list:
    """Возвращает ближайшую пару"""
    url = 'near_lesson/get_near_lesson/'
    data = {
        'schedule': schedule,
        'week': week,
    }
    near_lessons = get_api_data(url=url, data=data)

    return near_lessons


def get_now_lesson(schedule: list, week: str) -> list:
    """"Возвращает текущую пару"""
    url = 'near_lesson/get_now_lesson/'
    data = {
        'schedule': schedule,
        'week': week,
    }
    now_lessons = get_api_data(url=url, data=data)

    return now_lessons


def get_now_lesson_in_str_stud(now_lessons: list) -> str:
    """"Возвращает текущую пару как строку"""
    url = 'creating_schedule/get_now_lesson_in_str_stud/'
    data = {
        'now_lessons': now_lessons,
    }
    now_lessons_str = get_api_data(url=url, data=data)

    return now_lessons_str


def get_now_lesson_in_str_prep(now_lessons: list) -> str:
    """"Возвращает текущую пару как строку"""
    url = 'creating_schedule/get_now_lesson_in_str_prep/'
    data = {
        'now_lessons': now_lessons,
    }
    now_lessons_str = get_api_data(url=url, data=data)

    return now_lessons_str


def calculating_reminder_times(schedule, time: int) -> list:
    """Прощитывает время уведомления перед кадой парой"""
    url = 'notifications/calculating_reminder_times/'
    data = {
        'schedule': schedule,
        'time': time,
    }
    reminders = get_api_data(url=url, data=data)
    return reminders


def get_notifications_status(time):
    """Статус напоминаний"""
    url = 'notifications/get_notifications_status/'
    data = {
        'time': time
    }
    notifications_status = get_api_data(url=url, data=data)

    return notifications_status


class APIError:
    def __init__(self, error_msg=None):
        self.error_msg = error_msg
