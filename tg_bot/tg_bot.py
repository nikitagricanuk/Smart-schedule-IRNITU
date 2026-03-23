import os
import time

import pytz
import telebot

from actions import commands
from actions.main_menu import schedule, reminders, main_menu
from actions.registration import student_registration, teacher_registration
from actions.search.prep_and_group_search import start_search, handler_buttons, search
from actions.search.aud_search import start_search_aud, handler_buttons_aud, handler_buttons_aud_all_results

from tools.logger import logger
from tools.storage import MongodbService
from tools.keyboards import *

from tools import statistics

TG_TOKEN = os.environ.get('TG_TOKEN')

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')

bot = telebot.TeleBot(TG_TOKEN)

storage = MongodbService().get_instance()

content_schedule = ['Расписание 🗓', 'Ближайшая пара ⏱', 'Расписание на сегодня 🍏', 'На текущую неделю',
                    'На следующую неделю',
                    'Расписание на завтра 🍎', 'Следующая', 'Текущая', 'Экзамены']

content_main_menu_buttons = ['Основное меню', '<==Назад', 'Другое ⚡']

content_students_registration = ['institute', 'course', 'group']
content_reminder_settings = ['notification_btn', 'del_notifications', 'add_notifications', 'save_notifications']
content_prep_group = ["found_prep", "prep_list"]
content_aud = ["search_aud", "menu_aud"]


# ==================== Обработка команд ==================== #

# Команда /start
@bot.message_handler(func=lambda message: message.text in ['Начать', 'начать', 'Старт', 'старт', '/start', 'start'],
                     content_types=['text'])
def start_handler(message):
    commands.start(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /reg
@bot.message_handler(func=lambda message: message.text in ['Регистрация', 'регистрация', '/reg', 'reg'],
                     content_types=['text'])
def registration_handler(message):
    commands.registration(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /help
@bot.message_handler(func=lambda message: message.text in ['Помощь', 'помощь', '/help', 'help'], content_types=['text'])
def help_handler(message):
    commands.help_info(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /map Карта

@bot.message_handler(func=lambda message: message.text in ['Карта', 'карта', '/map', 'map'], content_types=['text'])
def map_handler(message):
    commands.show_map(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /about
@bot.message_handler(func=lambda message: message.text in ['О проекте', 'о проекте', '/about', 'about'],
                     content_types=['text'])
def about_handler(message):
    commands.about(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /authors
@bot.message_handler(func=lambda message: message.text in ['Авторы', 'авторы', '/authors', 'authors'],
                     content_types=['text'])
def authors_handler(message):
    commands.authors(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# Команда /tip
@bot.message_handler(func=lambda message: message.text in ['Подсказка', 'подсказка', 'tip', '/tip'],
                     content_types=['text'])
def authors_handler(message):
    commands.tip(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


# ==================== Обработка Inline кнопок ==================== #
@bot.callback_query_handler(func=lambda message: any(word in message.data for word in content_students_registration))
def student_registration_handler(message):
    """Регистрация студентов"""
    data = message.data
    if data == '{"institute": "Преподаватель"}':
        teacher_registration.start_prep_reg(bot=bot, message=message, storage=storage)
    else:
        student_registration.start_student_reg(bot=bot, message=message, storage=storage)
    logger.info(f'Inline button data: {data}')


@bot.message_handler(func=lambda message: message.text == 'Поиск 🔎', content_types=['text'])
def reminders_info_handler(message):
    """Начало поиска"""
    chat_id = message.chat.id
    bot.send_message(chat_id=chat_id, text='Выберите, что будем искать',
                     reply_markup=make_keyboard_search_goal())


@bot.message_handler(func=lambda message: message.text == 'Группы и преподаватели' or message.text == 'Аудитории',
                     content_types=['text'])
def reminders_info_handler(message):
    """Выбор поиска"""
    data = message.chat.id
    if message.text == "Группы и преподаватели":
        bot.send_message(chat_id=data, text='Вы выбрали поиск по группам и преподавателям',
                         reply_markup=make_keyboard_empty())
        start_search(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    else:
        bot.send_message(chat_id=data, text='Вы выбрали поиск по аудиториям',
                         reply_markup=make_keyboard_empty())
        start_search_aud(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    logger.info(f'Inline button data: {data}')


@bot.callback_query_handler(func=lambda message: 'prep_id' in message.data)
def prep_registration_handler(message):
    teacher_registration.reg_prep_choose_from_list(bot=bot, message=message, storage=storage)


@bot.callback_query_handler(func=lambda message: any(word in message.data for word in content_reminder_settings))
def reminder_settings_handler(message):
    data = message.data
    reminders.reminder_settings(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    logger.info(f'Inline button data: {data}')


@bot.callback_query_handler(func=lambda message: any(word in message.data for word in content_prep_group))
def prep_registration_handler(message):
    data = message.data
    handler_buttons(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    logger.info(f'Inline button data: {data}')


@bot.callback_query_handler(func=lambda message: any(word in message.data for word in content_aud))
def prep_registration_handler(message):
    data = message.data
    handler_buttons_aud(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
    logger.info(f'Inline button data: {data}')


@bot.message_handler(func=lambda message: message.text in content_schedule, content_types=['text'])
def schedule_handler(message):
    """Расписание"""
    schedule.get_schedule(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@bot.message_handler(func=lambda message: message.text == 'Напоминание 📣', content_types=['text'])
def reminders_info_handler(message):
    """Напоминания"""
    reminders.reminder_info(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@bot.message_handler(func=lambda message: message.text in content_main_menu_buttons, content_types=['text'])
def main_menu_buttons_handler(message):
    """Основные кнопки главного меню"""
    main_menu.processing_main_buttons(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)


@bot.callback_query_handler(func=lambda call: True)
def callback_inline(message):
    data = message.data
    if data != 'None':
        handler_buttons_aud_all_results(bot=bot, message=message, storage=storage, tz=TZ_IRKUTSK)
        logger.info(f'Inline button data: {data}')


# ==================== Обработка текста ==================== #
@bot.message_handler(content_types=['text'])
def text(message):
    chat_id = message.chat.id
    data = message.text
    user = storage.get_user(chat_id=chat_id)
    logger.info(f'Message data: {data}')

    if user:
        bot.send_message(chat_id, text='Я вас не понимаю 😞\n'
                                       'Для вызова подсказки используйте команду [Подсказка]\n'
                                       'Для просмотра списка команд используйте команду [Помощь]\n',
                         reply_markup=make_keyboard_start_menu())
    else:
        bot.send_message(chat_id, text='Я вас не понимаю 😞\n'
                                       'Похоже Вы не завершили регистрацию\n'
                                       'Чтобы использовать меня, завершите ее🙏')

    statistics.add(action='bullshit', storage=storage, tz=TZ_IRKUTSK)


if __name__ == '__main__':
    for attempt in range(1, 6):
        try:
            bot.remove_webhook()
            break
        except Exception as exc:
            logger.error(f'Failed to remove webhook (attempt {attempt}/5): {exc}')
            time.sleep(3)
    logger.info('Бот запущен...')
    while True:
        try:
            bot.infinity_polling(none_stop=True)
        except Exception as e:
            logger.error(e)
            time.sleep(3)

