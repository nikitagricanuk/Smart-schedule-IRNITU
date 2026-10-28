import locale
import os
import platform
import threading
from datetime import datetime, timedelta
from typing import Literal

import pytz

from tools.dated_lessons import lessons_on_date, time_minutes, transfer_note, week_parity
from tools.storage import MongodbService
from tools.logger import logger

VK_TOKEN = os.environ.get('VK')
TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')
locale_name = ('ru_RU.UTF-8' if platform.system() == 'Linux' else 'ru_RU')
locale.setlocale(locale.LC_TIME, locale_name)


def lessons_starting_at(schedule: list, moment: datetime) -> list:
    """Пары, которые начинаются ровно в ``moment``, с учётом разовых переносов.

    Пара, перенесённая на другую дату/время/аудиторию, напоминается в своё новое
    время и с новой аудиторией; на старом месте напоминания нет.
    """
    day = moment.date()
    minutes = moment.hour * 60 + moment.minute
    return [
        lesson for lesson in lessons_on_date(schedule, day, week_parity(day))
        if not lesson.get('moved_away')
        and lesson.get('name') != 'свободно'
        and time_minutes(lesson.get('time')) == minutes
    ]


def reminder_text(lessons: list, notifications: int) -> str:
    text = f'Через {notifications} минут пара\n'
    for lesson in lessons:
        text += '-------------------------------------------\n'
        aud = ', '.join(item for item in lesson.get('aud') or [] if item)
        text += f'Начало в {lesson["time"]}\n'
        if aud:
            text += f'Аудитория: {aud}\n'
        text += f'{lesson["name"]}\n' \
                f'{lesson.get("info", "")} {",".join(lesson.get("prep") or [])}\n'
        note = transfer_note(lesson)
        if note:
            text += f'{note}\n'
    text += '-------------------------------------------\n'
    return text


class Reminder:
    def __init__(self, bot_platform: Literal['vk', 'tg'], bot, storage=None, now=None):
        self.platform = bot_platform
        self.__check_platform()
        self.bot = bot
        self._storage = storage
        self.now = now or (lambda: datetime.now(TZ_IRKUTSK))
        self._last_minute = None

    @property
    def storage(self):
        # Подключение к базе — при первом обращении, а не при создании объекта.
        if self._storage is None:
            self._storage = MongodbService()
        return self._storage

    def due_reminders(self, users: list, now: datetime) -> list:
        """[(chat_id, текст)] для пользователей, у которых через ``notifications`` минут пара.

        Время считается от реальной даты пары, а не от заранее посчитанного
        расписания напоминаний по дням недели, поэтому перенос на другой день,
        время или аудиторию учитывается автоматически.
        """
        schedules = {}
        due = []
        for user in users:
            try:
                notifications = int(user.get('notifications') or 0)
            except (TypeError, ValueError):
                continue
            group = user.get('group')
            if notifications <= 0 or not group or not user.get('chat_id'):
                continue

            if group not in schedules:
                try:
                    schedules[group] = (self.storage.get_schedule(group=group) or {}).get('schedule') or []
                except Exception as e:
                    logger.exception(f'Error (group: {group}):\n{e}')
                    schedules[group] = []
            lessons = lessons_starting_at(schedules[group], now + timedelta(minutes=notifications))
            if lessons:
                due.append((user['chat_id'], reminder_text(lessons, notifications)))
        return due

    def send(self, chat_id, text):
        if self.platform == 'tg':
            try:
                self.bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:
                logger.exception(f'---TG---\n{e}')
        elif self.platform == 'vk':
            try:
                logger.info(f'vk send user_id: {chat_id}')
                self.bot.method('messages.send', {'user_id': chat_id, 'message': text, 'random_id': 0})
            except Exception as e:
                logger.exception(f'---VK---\n{e}')

    def __check_platform(self):
        """Проверка, что работает для такой платформы"""
        if self.platform == 'vk':
            logger.info('reminders_vk is started')
        elif self.platform == 'tg':
            logger.info('reminders_tg is started')
        else:
            raise ValueError(f'Нет такой платформы: {self.platform}')

    def get_users(self) -> list:
        if self.platform == 'vk':
            return self.storage.get_users_with_reminders_vk()
        return self.storage.get_users_with_reminders_tg()

    def search_for_reminders(self):
        # выполняем функцию каждые 60 секунд
        threading.Timer(60, self.search_for_reminders).start()

        now = self.now().replace(second=0, microsecond=0)
        # Таймер может сработать дважды за одну минуту — не отправляем одно напоминание два раза.
        if now == self._last_minute:
            return
        self._last_minute = now

        try:
            for chat_id, text in self.due_reminders(self.get_users(), now):
                self.send(chat_id, text)
        except Exception as e:
            logger.exception(f'{self.platform} reminders iteration failed: {e}')
