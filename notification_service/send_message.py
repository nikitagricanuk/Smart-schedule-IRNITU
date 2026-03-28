"""Запуск напоминаний от вк и tg в двух потоках"""
import os
from threading import Thread

import vk_api

from reminder import Reminder
from tools.aiogram_sync import SyncAiogramBot
from tools.reminder_updater import VKReminderUpdater, TGReminderUpdater

TG_TOKEN = os.environ.get('TG_TOKEN')
VK_TOKEN = os.environ.get('VK_TOKEN')

tg_bot = SyncAiogramBot(TG_TOKEN)
tg_reminder = Reminder(bot_platform='tg', bot=tg_bot)

vk_bot = vk_api.VkApi(token=VK_TOKEN)
vk_reminder = Reminder(bot_platform='vk', bot=vk_bot)

reminder_updater_vk = VKReminderUpdater()
reminder_updater_tg = TGReminderUpdater()


def main():
    tg = Thread(target=tg_reminder.search_for_reminders)
    vk = Thread(target=vk_reminder.search_for_reminders)

    vk_updater = Thread(target=reminder_updater_vk.start)
    tg_updater = Thread(target=reminder_updater_tg.start)

    tg.start()
    vk.start()
    vk_updater.start()
    tg_updater.start()


if __name__ == '__main__':
    main()
