import os
from typing import Optional

import aiohttp

from tools.logger import logger

FUNCTIONS_API_URL = os.environ.get('FUNCTIONS_API_URL')

_session: Optional[aiohttp.ClientSession] = None


async def init_http_session():
    global _session
    if _session is None or _session.closed:
        timeout = aiohttp.ClientTimeout(total=25)
        _session = aiohttp.ClientSession(timeout=timeout)


async def close_http_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None


async def get_api_data(url: str, data: dict = None):
    data = data or {}
    if _session is None or _session.closed:
        await init_http_session()
    try:
        async with _session.get(url=FUNCTIONS_API_URL + url, json=data) as answer:
            return await answer.json()
    except Exception as error:
        logger.exception(error)
        return APIError(error_msg=error)


async def find_week():
    return await get_api_data(url='find_week/')


async def schedule_view_exams(schedule: list) -> list:
    data = {'schedule': schedule['exams']['exams']}
    return await get_api_data(url='creating_schedule/schedule_view_exams/', data=data)


async def full_schedule_in_str(schedule: list, week: str) -> list:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='creating_schedule/full_schedule_in_str/', data=data)


async def get_one_day_schedule_in_str(schedule: list, week: str) -> str:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='creating_schedule/get_one_day_schedule_in_str/', data=data)


async def get_next_day_schedule_in_str(schedule: list, week: str) -> str:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='creating_schedule/get_next_day_schedule_in_str/', data=data)


async def get_one_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='creating_schedule/get_one_day_schedule_in_str_prep/', data=data)


async def get_next_day_schedule_in_str_prep(schedule: list, week: str) -> str:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='creating_schedule/get_next_day_schedule_in_str_prep/', data=data)


async def full_schedule_in_str_prep(schedule: list, week: str, aud=None) -> list:
    data = {'schedule': schedule, 'week': week, 'aud': aud}
    return await get_api_data(url='creating_schedule/full_schedule_in_str_prep/', data=data)


async def get_near_lesson(schedule: list, week: str) -> list:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='near_lesson/get_near_lesson/', data=data)


async def get_now_lesson(schedule: list, week: str) -> list:
    data = {'schedule': schedule, 'week': week}
    return await get_api_data(url='near_lesson/get_now_lesson/', data=data)


async def get_now_lesson_in_str_stud(now_lessons: list) -> str:
    data = {'now_lessons': now_lessons}
    return await get_api_data(url='creating_schedule/get_now_lesson_in_str_stud/', data=data)


async def get_now_lesson_in_str_prep(now_lessons: list) -> str:
    data = {'now_lessons': now_lessons}
    return await get_api_data(url='creating_schedule/get_now_lesson_in_str_prep/', data=data)


async def calculating_reminder_times(schedule, time: int) -> list:
    data = {'schedule': schedule, 'time': time}
    return await get_api_data(url='notifications/calculating_reminder_times/', data=data)


async def get_notifications_status(time):
    data = {'time': time}
    return await get_api_data(url='notifications/get_notifications_status/', data=data)


class APIError:
    def __init__(self, error_msg=None):
        self.error_msg = error_msg

