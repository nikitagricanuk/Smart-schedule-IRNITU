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
        answer = requests.get(url=request_url, json=payload, timeout=FUNCTIONS_API_TIMEOUT, verify=False)
        answer.raise_for_status()
        json_answer = answer.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f'functions_api request failed: url={request_url}, error={e}')
        return APIError(error_msg=e)

    return json_answer


def calculating_reminder_times(schedule, time: int) -> list:
    """Прощитывает время уведомления перед кадой парой"""
    url = 'notifications/calculating_reminder_times/'
    data = {
        'schedule': schedule,
        'time': time,
    }
    reminders = get_api_data(url=url, data=data)
    return reminders


class APIError:
    def __init__(self, error_msg=None):
        self.error_msg = error_msg
