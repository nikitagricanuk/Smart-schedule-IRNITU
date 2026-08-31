import os
from urllib.parse import quote

from tools import keyboards, statistics

CALENDAR_PUBLIC_BASE_URL = os.environ.get('CALENDAR_PUBLIC_BASE_URL', '').rstrip('/')


def _build_calendar_link(chat_id_user) -> str:
    if chat_id_user['course'] != 'None':
        path = f"/ical/group/{quote(chat_id_user['group'])}.ics"
    else:
        path = f"/ical/prep/{quote(chat_id_user['group'])}.ics"
    return f"{CALENDAR_PUBLIC_BASE_URL}{path}"


def send_calendar_subscription(bot, message, storage, tz):
    chat_id = message.chat.id
    user = storage.get_user(chat_id=chat_id)

    if not user or not user.get('group'):
        bot.send_message(
            chat_id=chat_id,
            text='Сначала завершите регистрацию, чтобы получить ссылку на календарь',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        return

    if not CALENDAR_PUBLIC_BASE_URL:
        bot.send_message(
            chat_id=chat_id,
            text='Подписка на календарь временно недоступна\nПопробуйте позже⏱',
            reply_markup=keyboards.make_keyboard_extra(),
        )
        return

    link = _build_calendar_link(user)
    bot.send_message(
        chat_id=chat_id,
        text='Скопируйте ссылку ниже и добавьте её как подписку на календарь '
             '(Google Calendar: "Другие календари" → "По URL", '
             'Apple Calendar: "Файл" → "Новая подписка")\n\n'
             f'{link}\n\n'
             'Расписание в календаре будет обновляться автоматически при изменениях в расписании ИРНИТУ.',
        reply_markup=keyboards.make_keyboard_extra(),
    )
    statistics.add(action='Подписка на календарь', storage=storage, tz=tz)
