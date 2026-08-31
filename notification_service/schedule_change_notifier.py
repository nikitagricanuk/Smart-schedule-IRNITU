"""Уведомляет пользователей Telegram об изменении расписания их группы."""

import time

from tools.logger import logger
from tools.storage import MongodbService

POLL_INTERVAL_SEC = 60


class ScheduleChangeNotifier:
    def __init__(self, bot, poll_interval_sec: int = POLL_INTERVAL_SEC, storage=None):
        self.bot = bot
        self.poll_interval_sec = poll_interval_sec
        self.storage = storage if storage is not None else MongodbService().get_instance()

    def _notify_group(self, group: str):
        users = self.storage.get_tg_users_for_group(group)
        text = (
            f'📅 Расписание группы {group} изменилось!\n'
            f'Откройте раздел "Расписание", чтобы увидеть актуальные пары.'
        )
        for user in users:
            chat_id = user.get('chat_id')
            if not chat_id:
                continue
            try:
                self.bot.send_message(chat_id=chat_id, text=text)
            except Exception as exc:
                logger.exception(f'Failed to send schedule change notification to {chat_id}: {exc}')

    def run(self):
        logger.info('schedule_change_notifier started')
        while True:
            try:
                for change in self.storage.get_pending_schedule_changes():
                    self._notify_group(change['group'])
                    self.storage.mark_schedule_change_notified(change['_id'])
            except Exception as exc:
                logger.exception(f'schedule_change_notifier iteration failed: {exc}')
            time.sleep(self.poll_interval_sec)
