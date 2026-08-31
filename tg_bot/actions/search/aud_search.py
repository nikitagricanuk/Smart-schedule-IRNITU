import json

from API.functions_api import APIError, find_week, full_schedule_in_str_prep
from tools import keyboards, schedule_processing
from tools.logger import logger

# chat_id -> {"page": int, "request_word": str, "results": list[str], "all_found_aud": list[str], "mode": str}
AUD_LIST = {}


def _parse_aud_callback(data):
    if not data:
        return None
    if data.startswith('aud:'):
        return data.split(':', 1)[1]
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None
    return payload.get('menu_aud')


async def start_search_aud(bot, message, storage, tz):  # noqa: ARG001
    chat_id = message.chat.id
    user = await storage.get_user(chat_id=chat_id)
    if user:
        AUD_LIST[chat_id] = {"page": 0, "request_word": "", "all_found_aud": [], "mode": "query"}
        await bot.send_message(
            chat_id=chat_id,
            text='Введите интересующую аудитрию\nНапример: Ж-317, или Ж317',
            reply_markup=keyboards.make_keyboard_main_menu(mode='aud'),
        )
        return

    institutes = await storage.get_institutes()
    if not institutes:
        logger.warning('Auditory search start requested while institutes are not loaded yet. chat_id=%s', chat_id)
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


async def _send_aud_schedule(bot, chat_id, request_word, storage, week_command):
    request_aud = await storage.get_schedule_aud(request_word)
    if not request_aud:
        await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
        return
    schedule = request_aud[0]['schedule']
    week = await find_week()
    if week_command == 'На следующую неделю':
        week = 'odd' if week == 'even' else 'even'
    week_name = 'четная' if week == 'odd' else 'нечетная'

    schedule_str = await full_schedule_in_str_prep(schedule, week=week, aud=request_word)
    if isinstance(schedule_str, APIError):
        await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
        return

    await bot.send_message(
        chat_id=chat_id,
        text=f'Расписание {request_word}\nНеделя: {week_name}',
        reply_markup=keyboards.make_keyboard_start_menu(),
    )
    await schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_str)


async def process_search_aud_message(bot, message, storage, tz):  # noqa: ARG001
    chat_id = message.chat.id
    if chat_id not in AUD_LIST:
        return False

    text = (message.text or '').strip()
    state = AUD_LIST[chat_id]

    if text == 'Основное меню':
        await bot.send_message(chat_id=chat_id, text='Вы покинули поиск', reply_markup=keyboards.make_keyboard_start_menu())
        AUD_LIST.pop(chat_id, None)
        return True

    if state.get("mode") == "week" and text in {'На текущую неделю', 'На следующую неделю'}:
        await _send_aud_schedule(bot, chat_id, state["request_word"], storage, text)
        AUD_LIST.pop(chat_id, None)
        return True

    direct = await storage.get_schedule_aud(text)
    all_results = []
    if not direct and len(text.replace(' ', '')) < 15:
        prep_list = []
        for item in text:
            for found in await storage.get_schedule_aud(item):
                prep_list.append(found['aud'])
        for item in set(prep_list):
            if text.replace(' ', '').lower() in item.replace('-', '').lower():
                all_results.append(item)

    if direct and not all_results:
        for item in direct:
            item['search_aud'] = item.pop('aud')
        found_aud = [item['search_aud'] for item in direct]
        AUD_LIST[chat_id] = {
            "page": 0,
            "request_word": text,
            "results": found_aud,
            "all_found_aud": [item.lower() for item in found_aud],
            "mode": "query",
        }
        shown = direct[:10] if len(direct) > 10 else direct
        await bot.send_message(
            chat_id=chat_id,
            text='Результат поиска',
            reply_markup=keyboards.make_keyboard_search_group_aud(
                last_request=direct[-1],
                page=0,
                more_than_10=len(direct) > 10,
                requests=shown,
                start_index=0,
            ),
        )
        return True

    if all_results:
        AUD_LIST[chat_id] = {
            "page": 0,
            "request_word": text,
            "results": all_results,
            "all_found_aud": [item.lower() for item in all_results],
            "mode": "query",
        }
        await bot.send_message(
            chat_id=chat_id,
            text='Результат поиска',
            reply_markup=keyboards.make_inline_keyboard_aud_results(all_results),
        )
        return True

    await bot.send_message(
        chat_id=chat_id,
        text='Проверьте правильность ввода 😕',
        reply_markup=keyboards.make_keyboard_main_menu(mode='aud'),
    )
    return True


async def handler_buttons_aud(bot, callback, storage, tz):  # noqa: ARG001
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    command = _parse_aud_callback(callback.data)

    if chat_id not in AUD_LIST:
        await callback.answer()
        return

    if command is None:
        await callback.answer()
        return

    state = AUD_LIST[chat_id]

    if command == 'main':
        await bot.send_message(chat_id=chat_id, text='Вы покинули поиск', reply_markup=keyboards.make_keyboard_start_menu())
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        AUD_LIST.pop(chat_id, None)
        await callback.answer()
        return

    page = state["page"]
    request_word = state["request_word"]
    request_aud = await storage.get_schedule_aud(request_word)
    for item in request_aud:
        item['search_aud'] = item.pop('aud')
    if request_aud:
        last_request = request_aud[-1]
    else:
        last_request = None

    selected_aud = None
    if command.isdigit():
        selected_index = int(command)
        results = state.get("results", [])
        if 0 <= selected_index < len(results):
            selected_aud = results[selected_index]
    elif command.lower() in state["all_found_aud"]:
        selected_aud = command

    if selected_aud:
        try:
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            pass
        state["request_word"] = selected_aud
        state["mode"] = "week"
        await bot.send_message(
            chat_id=chat_id,
            text=f'Выберите неделю для аудитории {selected_aud}',
            reply_markup=keyboards.make_keyboard_choose_schedule_for_aud_search(),
        )
        await callback.answer()
        return

    if command == 'back' and request_aud:
        new_page = page - 1
        shown = request_aud[10 * new_page:10 * (new_page + 1)]
        if new_page == 0:
            try:
                await bot.delete_message(message_id=message_id, chat_id=chat_id)
            except Exception:
                pass
            await bot.send_message(
                chat_id=chat_id,
                text='Результат поиска',
                reply_markup=keyboards.make_keyboard_search_group_aud(
                    last_request=last_request,
                    page=new_page,
                    requests=shown,
                    more_than_10=len(request_aud) > 10,
                    start_index=10 * new_page,
                ),
            )
        else:
            await bot.edit_message_reply_markup(
                message_id=message_id,
                chat_id=chat_id,
                reply_markup=keyboards.make_keyboard_search_group_aud(
                    last_request=last_request,
                    page=new_page,
                    requests=shown,
                    more_than_10=len(request_aud) > 10,
                    start_index=10 * new_page,
                ),
            )
        state["page"] = new_page
        await callback.answer()
        return

    if command == 'next' and request_aud:
        new_page = page + 1
        shown = request_aud[10 * new_page:10 * (new_page + 1)]
        try:
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            pass
        await bot.send_message(
            chat_id=chat_id,
            text='Результат поиска',
            reply_markup=keyboards.make_keyboard_search_group_aud(
                last_request=last_request,
                page=new_page,
                requests=shown,
                more_than_10=len(request_aud) > 10,
                start_index=10 * new_page,
            ),
        )
        state["page"] = new_page
        await callback.answer()
        return

    await bot.send_message(
        chat_id=chat_id,
        text='Проверьте правильность ввода 😞',
        reply_markup=keyboards.make_keyboard_main_menu(mode='aud'),
    )
    await callback.answer()
