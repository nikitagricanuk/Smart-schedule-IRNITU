import asyncio
import os
from typing import List

import pytz
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import CallbackQuery, Message

from API.functions_api import close_http_session, init_http_session
from actions import commands
from actions.main_menu import main_menu, reminders, schedule
from actions.registration import student_registration, teacher_registration
from actions.search.aud_search import AUD_LIST, handler_buttons_aud, process_search_aud_message, start_search_aud
from actions.search.prep_and_group_search import (
    CONDITION_REQUEST,
    handler_buttons,
    process_search_message,
    start_search,
)
from tools import statistics
from tools.keyboards import make_keyboard_empty, make_keyboard_search_goal, make_keyboard_start_menu
from tools.logger import logger
from tools.storage import MongodbService

TG_TOKEN = os.environ.get('TG_TOKEN')

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')
storage = MongodbService().get_instance()
router = Router()

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
global_command_texts = {
    'Начать',
    'начать',
    'Старт',
    'старт',
    '/start',
    'start',
    'Регистрация',
    'регистрация',
    '/reg',
    'reg',
    'Помощь',
    'помощь',
    '/help',
    'help',
    'Карта',
    'карта',
    '/map',
    'map',
    'О проекте',
    'о проекте',
    '/about',
    'about',
    'Авторы',
    'авторы',
    '/authors',
    'authors',
    'Подсказка',
    'подсказка',
    'tip',
    '/tip',
}


def _callback_data_contains(callback: CallbackQuery, words: List[str]) -> bool:
    if not callback.data:
        return False
    return any(word in callback.data for word in words)


def _callback_data_starts_with(callback: CallbackQuery, prefixes: List[str]) -> bool:
    if not callback.data:
        return False
    return any(callback.data.startswith(prefix) for prefix in prefixes)


@router.message(
    lambda message: message.chat.id in teacher_registration.WAITING_TEACHER_NAME
    and bool(message.text)
    and message.text not in global_command_texts
)
async def teacher_name_input_handler(message: Message, bot: Bot):
    handled = await teacher_registration.process_teacher_name_input(bot=bot, message=message, storage=storage)
    if handled:
        return


@router.message(
    lambda message: message.chat.id in CONDITION_REQUEST
    and bool(message.text)
    and message.text not in global_command_texts
)
async def search_state_message_handler(message: Message, bot: Bot):
    handled = await process_search_message(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    if handled:
        return


@router.message(
    lambda message: message.chat.id in AUD_LIST
    and bool(message.text)
    and message.text not in global_command_texts
)
async def aud_search_state_message_handler(message: Message, bot: Bot):
    handled = await process_search_aud_message(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    if handled:
        return


@router.message(F.text.in_(['Начать', 'начать', 'Старт', 'старт', '/start', 'start']))
async def start_handler(message: Message, bot: Bot):
    await commands.start(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['Регистрация', 'регистрация', '/reg', 'reg']))
async def registration_handler(message: Message, bot: Bot):
    teacher_registration.WAITING_TEACHER_NAME.discard(message.chat.id)
    CONDITION_REQUEST.pop(message.chat.id, None)
    AUD_LIST.pop(message.chat.id, None)
    await commands.registration(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['Помощь', 'помощь', '/help', 'help']))
async def help_handler(message: Message, bot: Bot):
    await commands.help_info(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['Карта', 'карта', '/map', 'map']))
async def map_handler(message: Message, bot: Bot):
    await commands.show_map(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['О проекте', 'о проекте', '/about', 'about']))
async def about_handler(message: Message, bot: Bot):
    await commands.about(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['Авторы', 'авторы', '/authors', 'authors']))
async def authors_handler(message: Message, bot: Bot):
    await commands.authors(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(['Подсказка', 'подсказка', 'tip', '/tip']))
async def tip_handler(message: Message, bot: Bot):
    await commands.tip(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text == 'Поиск 🔎')
async def search_start_handler(message: Message, bot: Bot):
    await bot.send_message(
        chat_id=message.chat.id,
        text='Выберите, что будем искать',
        reply_markup=make_keyboard_search_goal(),
    )


@router.message(F.text.in_(['Группы и преподаватели', 'Аудитории']))
async def search_select_handler(message: Message, bot: Bot):
    chat_id = message.chat.id
    if message.text == 'Группы и преподаватели':
        await bot.send_message(
            chat_id=chat_id,
            text='Вы выбрали поиск по группам и преподавателям',
            reply_markup=make_keyboard_empty(),
        )
        await start_search(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    else:
        await bot.send_message(
            chat_id=chat_id,
            text='Вы выбрали поиск по аудиториям',
            reply_markup=make_keyboard_empty(),
        )
        await start_search_aud(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.callback_query(
    lambda callback: _callback_data_contains(callback, ['institute', 'course', 'group'])
    or _callback_data_starts_with(callback, ['inst:', 'course:', 'group:'])
)
async def student_registration_handler(callback: CallbackQuery, bot: Bot):
    data = callback.data or ''
    if data in {'inst:teacher', '{"institute":"Преподаватель"}', '{"institute": "Преподаватель"}'}:
        await teacher_registration.start_prep_reg(bot=bot, callback=callback, storage=storage)
    else:
        await student_registration.start_student_reg(bot=bot, callback=callback, storage=storage)
    logger.info('Inline button data: %s', data)
    await callback.answer()


@router.callback_query(lambda callback: callback.data is not None and 'prep_id' in callback.data)
async def prep_registration_handler(callback: CallbackQuery, bot: Bot):
    await teacher_registration.reg_prep_choose_from_list(bot=bot, callback=callback, storage=storage)
    await callback.answer()


@router.callback_query(
    lambda callback: _callback_data_contains(
        callback,
        ['notification_btn', 'del_notifications', 'add_notifications', 'save_notifications'],
    )
)
async def reminder_settings_handler(callback: CallbackQuery, bot: Bot):
    await reminders.reminder_settings(bot=bot, callback=callback, storage=storage, tz=TZ_IRKUTSK)
    await callback.answer()


@router.callback_query(
    lambda callback: _callback_data_contains(callback, ['prep_list'])
    or _callback_data_starts_with(callback, ['prep:'])
)
async def prep_group_search_handler(callback: CallbackQuery, bot: Bot):
    await handler_buttons(bot=bot, callback=callback, storage=storage, tz=TZ_IRKUTSK)


@router.callback_query(
    lambda callback: _callback_data_contains(callback, ['menu_aud'])
    or _callback_data_starts_with(callback, ['aud:'])
)
async def aud_search_handler(callback: CallbackQuery, bot: Bot):
    await handler_buttons_aud(bot=bot, callback=callback, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(content_schedule))
async def schedule_handler(message: Message, bot: Bot):
    await schedule.get_schedule(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text == 'Напоминание 📣')
async def reminders_info_handler(message: Message, bot: Bot):
    await reminders.reminder_info(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text.in_(content_main_menu_buttons))
async def main_menu_buttons_handler(message: Message, bot: Bot):
    await main_menu.processing_main_buttons(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@router.message(F.text)
async def text_fallback(message: Message, bot: Bot):
    chat_id = message.chat.id
    data = message.text
    user = await storage.get_user(chat_id=chat_id)
    logger.info('Message data: %s', data)

    if user:
        await bot.send_message(
            chat_id,
            text='Я вас не понимаю 😞\n'
            'Для вызова подсказки используйте команду [Подсказка]\n'
            'Для просмотра списка команд используйте команду [Помощь]\n',
            reply_markup=make_keyboard_start_menu(),
        )
    else:
        await bot.send_message(
            chat_id,
            text='Я вас не понимаю 😞\n'
            'Похоже Вы не завершили регистрацию\n'
            'Чтобы использовать меня, завершите ее🙏',
        )

    await statistics.add(action='bullshit', storage=storage, tz=TZ_IRKUTSK)


async def main():
    if not TG_TOKEN:
        raise RuntimeError('Environment variable TG_TOKEN is required')

    bot = Bot(token=TG_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    logger.info('Бот запущен...')
    while True:
        try:
            await init_http_session()
            for attempt in range(1, 6):
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                    break
                except Exception as exc:
                    logger.error('Failed to remove webhook (attempt %s/5): %s', attempt, exc)
                    await asyncio.sleep(3)
            await dp.start_polling(bot)
            break
        except Exception as error:
            logger.exception(error)
            await asyncio.sleep(3)
        finally:
            await close_http_session()


if __name__ == '__main__':
    asyncio.run(main())
