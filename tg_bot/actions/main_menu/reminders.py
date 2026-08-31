import json

from API.functions_api import calculating_reminder_times, APIError
from API.functions_api import get_notifications_status
from tools import keyboards, statistics, schedule_processing
from tools.logger import logger


async def reminder_info(bot, message, storage, tz):
    chat_id = message.chat.id
    user = await storage.get_user(chat_id=chat_id)
    if not user:
        await bot.send_message(chat_id=chat_id, text='Сначала пройдите регистрацию с помощью команды /start')
        return

    time = user['notifications']
    if not time:
        time = 0

    # Проверяем статус напоминания
    notifications_status = await get_notifications_status(time)
    if isinstance(notifications_status, APIError):
        await schedule_processing.sending_service_is_not_available(bot, chat_id)
        return

    if user:
        await bot.send_message(
            chat_id=chat_id,
            text=notifications_status,
            reply_markup=keyboards.make_inline_keyboard_notifications(time),
        )

        await statistics.add(action='Напоминания', storage=storage, tz=tz)


async def reminder_settings(bot, callback, storage, tz):
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    data = callback.data

    if 'notification_btn' in data:
        data = json.loads(data)
        if data['notification_btn'] == 'close':
            try:
                await bot.delete_message(message_id=message_id, chat_id=chat_id)
                return
            except Exception as e:
                logger.exception(e)
                return
        time = data['notification_btn']

        # Проверяем статус напоминания
        notifications_status = await get_notifications_status(time)
        if isinstance(notifications_status, APIError):
            await schedule_processing.sending_service_is_not_available(bot, chat_id)
            return

        try:
            await bot.edit_message_text(
                message_id=message_id,
                chat_id=chat_id,
                text='Настройка напоминаний ⚙\n\n'
                'Укажите за сколько минут до начала пары должно приходить сообщение',
                reply_markup=keyboards.make_inline_keyboard_set_notifications(time),
            )
        except Exception as e:
            logger.exception(e)
            return

    elif 'del_notifications' in data:
        data = json.loads(data)
        time = data['del_notifications']
        if time == 0:
            return
        time -= 5

        if time < 0:
            time = 0

        # Проверяем статус напоминания
        notifications_status = await get_notifications_status(time)
        if isinstance(notifications_status, APIError):
            await schedule_processing.sending_service_is_not_available(bot, chat_id)
            return

        try:
            await bot.edit_message_reply_markup(
                message_id=message_id,
                chat_id=chat_id,
                reply_markup=keyboards.make_inline_keyboard_set_notifications(time),
            )
        except Exception as e:
            logger.exception(e)
            return

    elif 'add_notifications' in data:
        data = json.loads(data)
        time = data['add_notifications']
        time += 5

        # Проверяем статус напоминания
        notifications_status = await get_notifications_status(time)
        if isinstance(notifications_status, APIError):
            await schedule_processing.sending_service_is_not_available(bot, chat_id)
            return

        try:
            await bot.edit_message_reply_markup(
                message_id=message_id,
                chat_id=chat_id,
                reply_markup=keyboards.make_inline_keyboard_set_notifications(time),
            )
        except Exception as e:
            logger.exception(e)
            return

    elif 'save_notifications' in data:
        data = json.loads(data)
        time = data['save_notifications']

        # Проверяем статус напоминания
        notifications_status = await get_notifications_status(time)
        if isinstance(notifications_status, APIError):
            await schedule_processing.sending_service_is_not_available(bot, chat_id)
            return

        user = await storage.get_user(chat_id=chat_id)
        group = user['group']

        if user['course'] == 'None':
            schedule = (await storage.get_schedule_prep(group=group))['schedule']
        else:
            schedule = (await storage.get_schedule(group=group))['schedule']

        if time > 0:
            reminders = await calculating_reminder_times(schedule=schedule, time=int(time))
        else:
            reminders = []
        await storage.save_or_update_user(chat_id=chat_id, notifications=time, reminders=reminders)

        try:
            current_status = await get_notifications_status(time)
            await bot.edit_message_text(
                message_id=message_id,
                chat_id=chat_id,
                text=current_status,
                reply_markup=keyboards.make_inline_keyboard_notifications(time),
            )
        except Exception as e:
            logger.exception(e)
            return

        await statistics.add(action='save_notifications', storage=storage, tz=tz)
