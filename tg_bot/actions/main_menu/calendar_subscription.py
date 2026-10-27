import os
from urllib.parse import quote

from tools import keyboards, statistics

CALENDAR_PUBLIC_BASE_URL = os.environ.get('CALENDAR_PUBLIC_BASE_URL', '').rstrip('/')


def _build_calendar_link(token: str) -> str:
    """Личная ссылка: подгруппа и скрытые занятия берутся из настроек пользователя
    при каждом обновлении календаря, поэтому ссылка не меняется при их изменении."""
    return f"{CALENDAR_PUBLIC_BASE_URL}/ical/u/{quote(token)}.ics"


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

    link = _build_calendar_link(storage.get_or_create_calendar_token(chat_id))

    subgroup = int(user.get('subgroup') or 0)
    is_student = user.get('course', 'None') != 'None'
    if is_student:
        if subgroup in (1, 2, 3):
            subgroup_line = (f'\n\nКалендарь отфильтрован по подгруппе {subgroup}. '
                             'Изменить — кнопка [Подгруппа 👥].')
        else:
            subgroup_line = ('\n\nСейчас в календаре все занятия группы. Чтобы оставить только свою '
                             'подгруппу, выберите её кнопкой [Подгруппа 👥].')
    else:
        subgroup_line = ''

    hidden_count = len(user.get('hidden_lesson_types') or []) + len(user.get('hidden_lessons') or [])
    if hidden_count:
        hidden_line = f'\n\nСкрыто правил: {hidden_count}. Изменить — кнопка [Скрыть занятия 🙈].'
    else:
        hidden_line = '\n\nЛишние занятия (например, «Проект») можно убрать кнопкой [Скрыть занятия 🙈].'

    bot.send_message(
        chat_id=chat_id,
        text='Скопируйте ссылку ниже и добавьте её как подписку на календарь '
             '(Google Calendar: "Другие календари" → "По URL", '
             'Apple Calendar: "Файл" → "Новая подписка")\n\n'
             f'{link}\n\n'
             'Расписание в календаре будет обновляться автоматически при изменениях в расписании ИРНИТУ.'
             f'{subgroup_line}{hidden_line}\n\n'
             'Ссылка личная — не делитесь ей. Если вы подписывались на календарь раньше, '
             'замените старую подписку этой ссылкой.',
        reply_markup=keyboards.make_keyboard_extra(),
    )
    statistics.add(action='Подписка на календарь', storage=storage, tz=tz)
