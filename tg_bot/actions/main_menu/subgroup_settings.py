"""Выбор подгруппы. Влияет только на подписку на календарь — расписание в боте
всегда показывается полностью."""

import json

from actions.registration import student_registration
from tools import keyboards, statistics
from tools.logger import logger

_LABELS = dict(keyboards.SUBGROUP_OPTIONS)


def _subgroup_label(value: int) -> str:
    return _LABELS.get(value, 'Вся группа')


def choose_subgroup(bot, message, storage, tz):
    """Кнопка [Подгруппа 👥] — показывает текущий выбор и клавиатуру выбора."""
    chat_id = message.chat.id
    user = storage.get_user(chat_id=chat_id)

    if not user or not user.get('group'):
        bot.send_message(
            chat_id=chat_id,
            text='Сначала завершите регистрацию, чтобы выбрать подгруппу',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        return

    if user.get('course', 'None') == 'None':
        bot.send_message(
            chat_id=chat_id,
            text='Подгруппа есть только у студенческих групп',
            reply_markup=keyboards.make_keyboard_extra(),
        )
        return

    current = int(user.get('subgroup') or 0)
    bot.send_message(
        chat_id=chat_id,
        text=f'Текущая подгруппа для календаря: {_subgroup_label(current)}.\n\n'
             'Выберите подгруппу — по ней будет отфильтрована подписка на календарь. '
             'На расписание в боте это не влияет.',
        reply_markup=keyboards.make_inline_keyboard_choose_subgroup(current=current),
    )
    statistics.add(action='Выбор подгруппы', storage=storage, tz=tz)


def handle_subgroup_callback(bot, message, storage, tz):
    """Обрабатывает нажатие кнопки выбора подгруппы (callback_data {"subgroup": N[, "reg": 1]})."""
    chat_id = message.message.chat.id
    message_id = message.message.message_id

    try:
        data = json.loads(message.data)
    except (TypeError, ValueError):
        return

    subgroup = data.get('subgroup', 0)
    subgroup = subgroup if subgroup in (0, 1, 2, 3) else 0
    storage.set_subgroup(chat_id, subgroup)
    statistics.add(action=f'Подгруппа: {subgroup}', storage=storage, tz=tz)

    # Шаг регистрации — завершаем её приветственным сообщением.
    if data.get('reg'):
        try:
            bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception as e:
            logger.exception(e)
        student_registration.finish_registration(bot=bot, chat_id=chat_id)
        return

    try:
        bot.edit_message_text(
            message_id=message_id, chat_id=chat_id,
            text=f'Подгруппа для календаря: {_subgroup_label(subgroup)}.\n\n'
                 'Если подписка на календарь уже добавлена в приложение, обновите её '
                 '(или удалите и добавьте ссылку из раздела [Подписка на календарь 📅] заново).',
            reply_markup=keyboards.make_inline_keyboard_choose_subgroup(current=subgroup),
        )
    except Exception as e:
        # «message is not modified» при повторном выборе того же значения — не ошибка.
        logger.debug(f'subgroup edit skipped: {e}')
