"""Запуск сервиса напоминаний для Telegram и VK."""

import os
from threading import Thread

import vk_api

from reminder import Reminder
from tools.aiogram_sync import SyncAiogramBot
from tools.logger import logger
from tools.reminder_updater import TGReminderUpdater, VKReminderUpdater


def get_env(name: str) -> str:
    return (os.environ.get(name) or '').strip()


def build_workers():
    workers = []
    resources = []

    tg_token = get_env('TG_TOKEN')
    if tg_token:
        tg_bot = SyncAiogramBot(tg_token)
        tg_reminder = Reminder(bot_platform='tg', bot=tg_bot)
        workers.append(Thread(target=tg_reminder.search_for_reminders, name='tg_reminder'))
        workers.append(Thread(target=TGReminderUpdater().start, name='tg_reminder_updater'))
        resources.append(tg_bot)
    else:
        logger.warning('TG_TOKEN is not set, Telegram reminders are disabled')

    vk_token = get_env('VK_TOKEN')
    if vk_token:
        vk_bot = vk_api.VkApi(token=vk_token)
        vk_reminder = Reminder(bot_platform='vk', bot=vk_bot)
        workers.append(Thread(target=vk_reminder.search_for_reminders, name='vk_reminder'))
        workers.append(Thread(target=VKReminderUpdater().start, name='vk_reminder_updater'))
    else:
        logger.warning('VK_TOKEN is not set, VK reminders are disabled')

    if not workers:
        raise RuntimeError('No reminder transports configured. Set TG_TOKEN and/or VK_TOKEN.')

    return workers, resources


def main():
    workers, resources = build_workers()

    try:
        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        logger.info('notification_service stopped')
    finally:
        for resource in resources:
            try:
                resource.close()
            except Exception as exc:
                logger.exception(exc)


if __name__ == '__main__':
    main()
