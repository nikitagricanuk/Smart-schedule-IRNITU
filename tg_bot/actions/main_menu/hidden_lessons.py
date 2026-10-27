"""Скрытие занятий: целых видов ('Проект') и отдельных предметов.

Правила применяются только к личной подписке на календарь — расписание в боте
всегда показывается полностью.
callback_data: 'hide:t:<i>' / 'hide:l:<i>' — индекс в списке type_options /
lesson_options текущего расписания (названия не влезают в 64 байта), 'hide:reset'.
"""

from tools import keyboards, lesson_filters, statistics
from tools.logger import logger

CALLBACK_PREFIX = 'hide:'

_INTRO = ('Нажмите на занятие, чтобы скрыть его или вернуть обратно.\n\n'
          'Сверху — виды занятий: скрывают все пары этого вида (например, все «Проект»).\n'
          'Ниже — отдельные предметы.\n\n'
          'Скрытые занятия пропадают из подписки на календарь (расписание в боте не меняется). '
          'Календарь подхватит изменения при следующем обновлении подписки.')


def _load(storage, chat_id):
    user = storage.get_user(chat_id=chat_id)
    if not user or not user.get('group'):
        return user, None
    if user.get('course', 'None') != 'None':
        schedule_doc = storage.get_schedule(group=user['group'])
    else:
        schedule_doc = storage.get_schedule_prep(group=user['group'])
    return user, (schedule_doc or {}).get('schedule') or []


def _keyboard(user, schedule):
    hidden_types, hidden_lessons = lesson_filters.user_rules(user)
    types_ = [(i, ltype, ltype in hidden_types) for i, ltype in enumerate(lesson_filters.type_options(schedule))]
    # Предметы скрытого целиком вида не показываем — переключать их отдельно бессмысленно.
    lessons = [(i, lesson_filters.lesson_label(key), key in hidden_lessons)
               for i, key in enumerate(lesson_filters.lesson_options(schedule))
               if key.rpartition('|')[2] not in hidden_types]
    return keyboards.make_inline_keyboard_hidden_lessons(types_, lessons, has_hidden=bool(hidden_types or hidden_lessons))


def show_hidden_lessons(bot, message, storage, tz):
    """Кнопка [Скрыть занятия 🙈]."""
    chat_id = message.chat.id
    user, schedule = _load(storage, chat_id)

    if schedule is None:
        bot.send_message(
            chat_id=chat_id,
            text='Сначала завершите регистрацию, чтобы настроить скрытые занятия',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        return
    if not schedule:
        bot.send_message(chat_id=chat_id, text='Расписание временно недоступно\nПопробуйте позже⏱',
                         reply_markup=keyboards.make_keyboard_extra())
        return

    bot.send_message(chat_id=chat_id, text=_INTRO, reply_markup=_keyboard(user, schedule))
    statistics.add(action='Скрыть занятия', storage=storage, tz=tz)


def handle_hidden_lessons_callback(bot, message, storage, tz):
    chat_id = message.message.chat.id
    message_id = message.message.message_id
    action = (message.data or '')[len(CALLBACK_PREFIX):]

    user, schedule = _load(storage, chat_id)
    if not schedule:
        return
    hidden_types, hidden_lessons = lesson_filters.user_rules(user)

    if action == 'reset':
        hidden_types, hidden_lessons = set(), set()
    else:
        kind, _, index = action.partition(':')
        options = {'t': (lesson_filters.type_options(schedule), hidden_types),
                   'l': (lesson_filters.lesson_options(schedule), hidden_lessons)}.get(kind)
        if options is None or not index.isdigit() or int(index) >= len(options[0]):
            return
        value, target = options[0][int(index)], options[1]
        if value in target:
            target.discard(value)
        else:
            target.add(value)

    storage.set_hidden_lessons(chat_id, list(hidden_types), list(hidden_lessons))
    user = {**user, 'hidden_lesson_types': list(hidden_types), 'hidden_lessons': list(hidden_lessons)}
    statistics.add(action='Скрыть занятия: изменение', storage=storage, tz=tz)

    try:
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id,
                                      reply_markup=_keyboard(user, schedule))
    except Exception as e:
        # «message is not modified» при повторном «Показать всё» — не ошибка.
        logger.debug(f'hidden lessons edit skipped: {e}')
