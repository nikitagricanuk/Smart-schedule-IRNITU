import asyncio
import os
from functools import partial

import pytz
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

from actions import commands
from actions.main_menu import main_menu, reminders, schedule
from actions.registration import student_registration, teacher_registration
from actions.search.aud_search import (
    handler_buttons_aud,
    handler_buttons_aud_all_results,
    start_search_aud,
)
from actions.search.prep_and_group_search import handler_buttons, start_search
from tools import statistics
from tools.aiogram_compat import AiogramTeleBotCompat, StepHandlerRegistry
from tools.config import require_env
from tools.keyboards import make_keyboard_empty, make_keyboard_search_goal, make_keyboard_start_menu
from tools.logger import logger
from tools.storage import MongodbService

TG_TOKEN = require_env('TG_TOKEN')

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')

storage = MongodbService().get_instance()
step_registry = StepHandlerRegistry()

aiogram_bot = Bot(token=TG_TOKEN)
dp = Dispatcher(aiogram_bot)
bot = AiogramTeleBotCompat(bot=aiogram_bot, step_registry=step_registry)

content_schedule = [
    'Расписание 🗓',
    'Ближайшая пара ⏱',
    'Расписание на сегодня 🍏',
    'На текущую неделю',
    'На следующую неделю',
    'Расписание на завтра 🍎',
    'Следующая',
    'Текущая',
    'Экзамены',
]

content_main_menu_buttons = ['Основное меню', '<==Назад', 'Другое ⚡']
content_students_registration = ['institute', 'course', 'group']
content_reminder_settings = ['notification_btn', 'del_notifications', 'add_notifications', 'save_notifications']
content_prep_group = ['found_prep', 'prep_list']
content_aud = ['search_aud', 'menu_aud']


async def run_sync(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    bound = partial(func, *args, **kwargs)
    return await loop.run_in_executor(None, bound)


def handle_registration_callback(callback_query):
    data = callback_query.data
    if data == '{"institute": "Преподаватель"}':
        teacher_registration.start_prep_reg(bot=bot, message=callback_query, storage=storage)
    else:
        student_registration.start_student_reg(bot=bot, message=callback_query, storage=storage)
    logger.info(f'Inline button data: {data}')


def handle_search_menu(message):
    chat_id = message.chat.id
    bot.send_message(chat_id=chat_id, text='Выберите, что будем искать', reply_markup=make_keyboard_search_goal())


def handle_search_choice(message):
    chat_id = message.chat.id

    if message.text == 'Группы и преподаватели':
        bot.send_message(
            chat_id=chat_id,
            text='Вы выбрали поиск по группам и преподавателям',
            reply_markup=make_keyboard_empty(),
        )
        start_search(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    else:
        bot.send_message(
            chat_id=chat_id,
            text='Вы выбрали поиск по аудиториям',
            reply_markup=make_keyboard_empty(),
        )
        start_search_aud(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)

    logger.info(f'Inline button data: {chat_id}')


def handle_unknown_text(message):
    chat_id = message.chat.id
    data = message.text
    user = storage.get_user(chat_id=chat_id)
    logger.info(f'Message data: {data}')

    if user:
        bot.send_message(
            chat_id,
            text='Я вас не понимаю 😞\n'
                 'Для вызова подсказки используйте команду [Подсказка]\n'
                 'Для просмотра списка команд используйте команду [Помощь]\n',
            reply_markup=make_keyboard_start_menu(),
        )
    else:
        bot.send_message(
            chat_id,
            text='Я вас не понимаю 😞\n'
                 'Похоже Вы не завершили регистрацию\n'
                 'Чтобы использовать меня, завершите ее🙏',
        )

    statistics.add(action='bullshit', storage=storage, tz=TZ_IRKUTSK)


@dp.message_handler(content_types=types.ContentTypes.ANY)
async def message_router(message: types.Message):
    pending_step = step_registry.pop(message.chat.id)
    if pending_step is not None:
        try:
            await run_sync(pending_step.callback, message, *pending_step.args, **pending_step.kwargs)
        except Exception as exc:
            logger.exception(exc)
        return

    if message.content_type != types.ContentType.TEXT:
        return

    text = message.text or ''

    try:
        if text in ['Начать', 'начать', 'Старт', 'старт', '/start', 'start']:
            await run_sync(commands.start, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['Регистрация', 'регистрация', '/reg', 'reg']:
            await run_sync(commands.registration, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['Помощь', 'помощь', '/help', 'help']:
            await run_sync(commands.help_info, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['Карта', 'карта', '/map', 'map']:
            await run_sync(commands.show_map, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['О проекте', 'о проекте', '/about', 'about']:
            await run_sync(commands.about, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['Авторы', 'авторы', '/authors', 'authors']:
            await run_sync(commands.authors, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in ['Подсказка', 'подсказка', 'tip', '/tip']:
            await run_sync(commands.tip, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text == 'Поиск 🔎':
            await run_sync(handle_search_menu, message)
        elif text in ['Группы и преподаватели', 'Аудитории']:
            await run_sync(handle_search_choice, message)
        elif text in content_schedule:
            await run_sync(schedule.get_schedule, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text == 'Напоминание 📣':
            await run_sync(reminders.reminder_info, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        elif text in content_main_menu_buttons:
            await run_sync(main_menu.processing_main_buttons, bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        else:
            await run_sync(handle_unknown_text, message)
    except Exception as exc:
        logger.exception(exc)


@dp.callback_query_handler(lambda callback_query: True)
async def callback_router(callback_query: types.CallbackQuery):
    data = callback_query.data or ''

    try:
        await callback_query.answer()
    except Exception:
        pass

    try:
        if any(word in data for word in content_students_registration):
            await run_sync(handle_registration_callback, callback_query)
        elif 'prep_id' in data:
            await run_sync(teacher_registration.reg_prep_choose_from_list, bot=bot, message=callback_query, storage=storage)
        elif any(word in data for word in content_reminder_settings):
            await run_sync(reminders.reminder_settings, bot=bot, message=callback_query, storage=storage, tz=TZ_IRKUTSK)
            logger.info(f'Inline button data: {data}')
        elif any(word in data for word in content_prep_group):
            await run_sync(handler_buttons, bot=bot, message=callback_query, storage=storage, tz=TZ_IRKUTSK)
            logger.info(f'Inline button data: {data}')
        elif any(word in data for word in content_aud):
            await run_sync(handler_buttons_aud, bot=bot, message=callback_query, storage=storage, tz=TZ_IRKUTSK)
            logger.info(f'Inline button data: {data}')
        elif data != 'None':
            await run_sync(
                handler_buttons_aud_all_results,
                bot=bot,
                message=callback_query,
                storage=storage,
                tz=TZ_IRKUTSK,
            )
            logger.info(f'Inline button data: {data}')
    except Exception as exc:
        logger.exception(exc)


async def on_startup(_):
    bot.attach_loop(asyncio.get_running_loop())

    for attempt in range(1, 6):
        try:
            await aiogram_bot.delete_webhook()
            break
        except Exception as exc:
            logger.error(f'Failed to remove webhook (attempt {attempt}/5): {exc}')
            await asyncio.sleep(3)

    logger.info('Бот запущен...')


async def on_shutdown(_):
    await aiogram_bot.session.close()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
