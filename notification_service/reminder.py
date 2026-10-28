import locale
import os
import platform
import threading
from datetime import datetime
from typing import Literal

import pytz

import tools
from tools.dated_lessons import lessons_on_date
from tools.storage import MongodbService
from tools.logger import logger

VK_TOKEN = os.environ.get('VK')
TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')
locale_name = ('ru_RU.UTF-8' if platform.system() == 'Linux' else 'ru_RU')
locale.setlocale(locale.LC_TIME, locale_name)


class Reminder:
    def __init__(self, bot_platform: Literal['vk', 'tg'], bot):
        self.platform = bot_platform
        self.__check_platform()
        self.bot = bot
        self.storage = MongodbService()
        self.users = []

    def sending_notifications(self):
        for user in self.users:
            chat_id = user['chat_id']
            week = user['week']
            time = user['time']
            group = user['group']
            notifications = user['notifications']

            try:
                schedule = self.storage.get_schedule(group=group)['schedule']
            except Exception as e:
                logger.exception(f'Error (group: {group}):\n{e}')
                continue

            # Пары сегодняшнего дня с учётом разовых переносов (перенесённые на другой день не напоминаем).
            lessons = [
                lesson for lesson in lessons_on_date(schedule, datetime.now(TZ_IRKUTSK).date(), week)
                if not lesson.get('moved_away')
            ]

            # если не нашлось переходем к след user
            if not lessons:
                continue

            lessons_for_reminders = tools.forming_message_text(lessons=lessons, week=week, time=time)

            # если пары не нашлись переходим к след user
            if not lessons_for_reminders:
                continue

            # Отправляем сообщение пользователю
            text = f'Через {notifications} минут пара\n' \
                   f'{lessons_for_reminders}'

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

    def search_for_reminders(self):
        logger.info(f'{self.platform} сработало')
        # определяем время сейчас
        time_now = datetime.now(TZ_IRKUTSK)
        day_now = datetime.now(TZ_IRKUTSK).strftime('%A').lower()

        # выполняем функцию каждые 60 секунд
        threading.Timer(60, self.search_for_reminders).start()

        # minutes_now = time_now.strftime('%M')

        self.users = []

        # получаем пользователей у которых включены напоминания
        if self.platform == 'vk':
            reminders = self.storage.get_users_with_reminders_vk()
        elif self.platform == 'tg':
            reminders = self.storage.get_users_with_reminders_tg()

        for reminder in reminders:
            week = tools.find_week()

            if 'reminders' not in reminder.keys():
                continue

            # если у пользователя пустой reminders то None
            user_days = reminder['reminders'].get(week)
            if not user_days:
                continue
            # если у пользователя нет ткущего дня, то None
            user_day_reminder_time = user_days.get(day_now.lower())

            # если время совпадает с текущим, добавляем в список на отправку
            if tools.check_that_user_has_reminder_enabled_for_the_current_time(time_now,
                                                                               user_day_reminder_time):
                chat_id = reminder['chat_id']
                group = reminder['group']
                notifications = reminder['notifications']

                user = tools.forming_user_to_submit(chat_id, group, notifications, day_now, time_now, week)
                self.users.append(user)

        # после того как список сформирован, нужно отправить его боту
        self.sending_notifications()
