import json

from API.functions_api import APIError, find_week
from API.functions_api import full_schedule_in_str, full_schedule_in_str_prep, schedule_view_exams
from tools import keyboards, schedule_processing
from tools.logger import logger

# chat_id -> {"page": int, "request_word": str, "results": list[str], "all_found_results": list[str], "mode": str}
CONDITION_REQUEST = {}


def _parse_prep_callback(data):
    if not data:
        return None
    if data.startswith('prep:'):
        return data.split(':', 1)[1]
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None
    return payload.get('prep_list')


async def _groups_exam(group, storage):
    schedule = await storage.get_schedule_exam(group=group)
    if not schedule:
        return None
    schedule.pop('_id', None)
    clear_list = []
    for exam in schedule['exams']['exams']:
        if exam not in clear_list:
            clear_list.append(exam)
    schedule['exams']['exams'] = clear_list
    return schedule


async def start_search(bot, message, storage, tz):  # noqa: ARG001
    chat_id = message.chat.id
    user = await storage.get_user(chat_id=chat_id)

    if user:
        CONDITION_REQUEST[chat_id] = {"page": 0, "request_word": "", "all_found_results": [], "mode": "query"}
        await bot.send_message(
            chat_id=chat_id,
            text='Введите название группы или фамилию преподавателя\nНапример: ИБб-18-1 или Маринов',
            reply_markup=keyboards.make_keyboard_main_menu(),
        )
        return

    institutes = await storage.get_institutes()
    if not institutes:
        logger.warning('Search start requested while institutes are not loaded yet. chat_id=%s', chat_id)
        await bot.send_message(
            chat_id=chat_id,
            text='Расписание и списки групп еще загружаются ⏳\n'
            'Попробуйте снова через 1-2 минуты.',
        )
        return

    await bot.send_message(chat_id=chat_id, text='Привет\n')
    await bot.send_message(chat_id=chat_id, text='Для начала пройдите небольшую регистрацию😉\n')
    await bot.send_message(
        chat_id=chat_id,
        text='Выберите институт',
        reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
    )


async def process_search_message(bot, message, storage, tz):  # noqa: ARG001
    chat_id = message.chat.id
    if chat_id not in CONDITION_REQUEST:
        return False

    text = (message.text or '').strip()
    state = CONDITION_REQUEST[chat_id]

    if text == 'Основное меню':
        await bot.send_message(chat_id=chat_id, text='Основное меню', reply_markup=keyboards.make_keyboard_start_menu())
        CONDITION_REQUEST.pop(chat_id, None)
        return True

    if state.get("mode") == "week" and text in {'На текущую неделю', 'На следующую неделю', 'Экзамены'}:
        request_word = state["request_word"]
        request_group = await storage.get_search_list(request_word)
        request_prep = await storage.get_search_list_prep(request_word)

        if text == 'Экзамены':
            schedule_doc = await _groups_exam(request_word, storage)
            if not schedule_doc:
                await bot.send_message(chat_id=chat_id, text='Расписание экзаменов отсутствует😇\nПопробуйте позже⏱')
                return True
            schedule_exams = await schedule_view_exams(schedule=schedule_doc)
            if isinstance(schedule_exams, APIError):
                await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
                return True
            await schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_exams)
            CONDITION_REQUEST.pop(chat_id, None)
            return True

        if request_group:
            schedule_doc = await storage.get_schedule(group=request_word)
        elif request_prep:
            schedule_doc = request_prep[0]
        else:
            schedule_doc = None

        if not schedule_doc:
            await bot.send_message(chat_id=chat_id, text='Расписание временно недоступно\nПопробуйте позже⏱')
            return True

        schedule = schedule_doc['schedule']
        week = await find_week()
        if text == 'На следующую неделю':
            week = 'odd' if week == 'even' else 'even'

        week_name = 'четная' if week == 'odd' else 'нечетная'
        if request_group:
            schedule_str = await full_schedule_in_str(schedule, week=week)
        else:
            schedule_str = await full_schedule_in_str_prep(schedule, week=week)

        if isinstance(schedule_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return True

        await bot.send_message(
            chat_id=chat_id,
            text=f'Расписание {request_word}\nНеделя: {week_name}',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        await schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_str)
        CONDITION_REQUEST.pop(chat_id, None)
        return True

    if not text:
        await bot.send_message(
            chat_id=chat_id,
            text='Проверьте правильность ввода 😞',
            reply_markup=keyboards.make_keyboard_main_menu(),
        )
        return True

    request_group = await storage.get_search_list(text)
    request_prep = await storage.get_search_list_prep(text)
    if not request_group and not request_prep:
        await bot.send_message(
            chat_id=chat_id,
            text='Проверьте правильность ввода 😞',
            reply_markup=keyboards.make_keyboard_main_menu(),
        )
        return True

    for item in request_group:
        item['found_prep'] = item.pop('name')
    for item in request_prep:
        item['found_prep'] = item.pop('prep_short_name')
    request = request_group + request_prep
    last_request = request[-1]
    all_found_results = [item['found_prep'].lower() for item in request]

    CONDITION_REQUEST[chat_id] = {
        "page": 0,
        "request_word": text,
        "results": [item['found_prep'] for item in request],
        "all_found_results": all_found_results,
        "mode": "query",
    }

    if len(request) > 10:
        shown = request[:10]
        more_than_10 = True
    else:
        shown = request
        more_than_10 = False

    await bot.send_message(
        chat_id=chat_id,
        text='Результат поиска',
        reply_markup=keyboards.make_keyboard_search_group(
            last_request=last_request,
            page=0,
            more_than_10=more_than_10,
            requests=shown,
            start_index=0,
        ),
    )
    return True


async def handler_buttons(bot, callback, storage, tz):  # noqa: ARG001
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    command = _parse_prep_callback(callback.data)

    if chat_id not in CONDITION_REQUEST:
        await callback.answer()
        return

    if command is None:
        await callback.answer()
        return

    state = CONDITION_REQUEST[chat_id]
    if command == 'main':
        await bot.send_message(chat_id=chat_id, text='Основное меню', reply_markup=keyboards.make_keyboard_start_menu())
        try:
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            pass
        CONDITION_REQUEST.pop(chat_id, None)
        await callback.answer()
        return

    page = state["page"]
    request_word = state["request_word"]
    request_group = await storage.get_search_list(request_word)
    request_prep = await storage.get_search_list_prep(request_word)
    for item in request_group:
        item['found_prep'] = item.pop('name')
    for item in request_prep:
        item['found_prep'] = item.pop('prep_short_name')
    request = request_group + request_prep
    if not request:
        await bot.send_message(chat_id=chat_id, text='Проверьте правильность ввода 😞')
        CONDITION_REQUEST.pop(chat_id, None)
        await callback.answer()
        return
    last_request = request[-1]

    selected_value = None
    if command.isdigit():
        selected_index = int(command)
        results = state.get("results", [])
        if 0 <= selected_index < len(results):
            selected_value = results[selected_index]
    elif command.lower() in state["all_found_results"]:
        selected_value = command

    if selected_value:
        selected = selected_value.lower()
        try:
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            pass
        state["request_word"] = selected_value
        state["mode"] = "week"
        keyboard = (
            keyboards.make_keyboard_choose_schedule()
            if "-" in selected
            else keyboards.make_keyboard_choose_schedule_for_aud_search()
        )
        await bot.send_message(chat_id=chat_id, text=f'Выберите неделю для {selected_value}', reply_markup=keyboard)
        await callback.answer()
        return

    if command == 'back':
        new_page = page - 1
        more_than_10 = len(request) > 10
        shown = request[10 * new_page:10 * (new_page + 1)]
        if new_page == 0:
            try:
                await bot.delete_message(message_id=message_id, chat_id=chat_id)
            except Exception:
                pass
            await bot.send_message(
                chat_id=chat_id,
                text='Первая страница поиска:',
                reply_markup=keyboards.make_keyboard_search_group(
                    last_request=last_request,
                    page=new_page,
                    requests=shown,
                    more_than_10=more_than_10,
                    start_index=10 * new_page,
                ),
            )
        else:
            await bot.edit_message_reply_markup(
                message_id=message_id,
                chat_id=chat_id,
                reply_markup=keyboards.make_keyboard_search_group(
                    last_request=last_request,
                    page=new_page,
                    requests=shown,
                    more_than_10=more_than_10,
                    start_index=10 * new_page,
                ),
            )
        state["page"] = new_page
        await callback.answer()
        return

    if command == 'next':
        new_page = page + 1
        more_than_10 = len(request) > 10
        shown = request[10 * new_page:10 * (new_page + 1)]
        try:
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            pass
        await bot.send_message(
            chat_id=chat_id,
            text='Следующая страница',
            reply_markup=keyboards.make_keyboard_search_group(
                last_request=last_request,
                page=new_page,
                requests=shown,
                more_than_10=more_than_10,
                start_index=10 * new_page,
            ),
        )
        state["page"] = new_page
        await callback.answer()
        return

    await bot.send_message(
        chat_id=chat_id,
        text='Проверьте правильность ввода 😞',
        reply_markup=keyboards.make_keyboard_main_menu(),
    )
    await callback.answer()
