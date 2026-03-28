import json

from API.functions_api import find_week
from API.functions_api import full_schedule_in_str_prep, APIError
from tools import keyboards, schedule_processing

# Глобальная переменная(словарь), которая хранит в себе 3 состояния
# (номер страницы; слово, которые находим; список соответствия для выхода по условию в стейте)
aud_list = {}


def start_search_aud(bot, message, storage, tz):
    # ID пользователя
    chat_id = message.chat.id
    # Создаём ключ по значению ID пользователя
    aud_list[chat_id] = []
    # Зарашиваем данные о пользователе из базы
    user = storage.get_user(chat_id=chat_id)
    # Условие для проверки наличия пользователя в базе

    if user:

        # Запуск стейта со значением SEARCH
        msg = bot.send_message(chat_id=chat_id, text='Введите интересующую аудитрию\n'
                                                     'Например: Ж-317, или Ж317',
                               reply_markup=keyboards.make_keyboard_main_menu())

        bot.register_next_step_handler(msg, search_aud, bot=bot, tz=tz, storage=storage)

    else:

        bot.send_message(chat_id=chat_id, text='Привет\n')
        bot.send_message(chat_id=chat_id, text='Для начала пройдите небольшую регистрацию😉\n')
        bot.send_message(chat_id=chat_id, text='Выберите институт',
                         reply_markup=keyboards.make_inline_keyboard_choose_institute(storage.get_institutes()))


def search_aud(message, bot, storage, tz, last_msg=None):
    """Регистрация преподавателя"""
    global aud_list
    chat_id = message.chat.id
    message_id = message.message_id
    data = message
    message = message.text
    all_found_aud = []
    all_results = []
    prep_list = []
    page = 0

    if data.content_type == 'sticker':
        message = ''

    if last_msg:
        bot.delete_message(data.chat.id, data.message_id - 1)

    if ('На текущую неделю' == message or 'На следующую неделю' == message):
        return

    if not storage.get_schedule_aud(message) and len(message.replace(' ', '')) < 15:
        # Отправляем запросы в базу посимвольно
        for item in message:
            # Получаем все результаты запроса на каждый символ
            request_item_all = storage.get_schedule_aud(item)
            # Проходим по каждому результату запроса одного символа
            for i in range(len(request_item_all)):
                # Обращаемся к результатам у которых есть ключ "aud"
                request_item = request_item_all[i]['aud']
                # Записывем все совпадения (Значения ключа "aud")
                prep_list.append(request_item)
                request_item = []

            request_item_all = []

        # Выделение наиболее повторяющихся элементов(а). Фактически результат запроса пользователя.
        qty_most_common = 0
        prep_list_set = set(prep_list)
        for item in prep_list_set:
            qty = prep_list.count(item)
            if qty > qty_most_common:
                qty_most_common = qty
                # Переменная с результатом сортировки
            if message.replace(' ', '').lower() in item.replace('-', '').lower():
                all_results.append(item.lower())

    if storage.get_schedule_aud(message) and not all_results:
        # Результат запроса по аудам
        request_aud = storage.get_schedule_aud(message)
        # Циклы нужны для общего поиска. Здесь мы удаляем старые ключи в обоих реквестах и создаём один общий ключ, как для групп, так и для преподов
        for i in request_aud:
            i['search_aud'] = i.pop('aud')
        # Записываем слово, которое ищем
        request_word = message

        last_request = request_aud[-1]
        # Эти циклы записывают группы и преподов в нижнем регистре для удобной работы с ними
        for i in request_aud:
            all_found_aud.append(i['search_aud'].lower())
        # Формируем полный багаж для пользователя
        list_search = [page, request_word, all_found_aud]
        # Записываем все данные под ключом пользователя
        aud_list[chat_id] = list_search
        # Выводим результат поиска с клавиатурой (кливиатур формируется по поисковому запросу)
        if len(request_aud) > 10:
            requests = request_aud[:10 * (page + 1)]
            more_than_10 = True
            msg = bot.send_message(chat_id=chat_id, text='Результат поиска',
                                   reply_markup=keyboards.make_keyboard_search_group_aud(last_request=last_request,
                                                                                         page=page,
                                                                                         more_than_10=more_than_10,
                                                                                         requests=requests))
            bot.register_next_step_handler(msg, search_aud, bot=bot, storage=storage, tz=tz, last_msg=msg)

        else:
            msg = bot.send_message(chat_id=chat_id, text='Результат поиска',
                                   reply_markup=keyboards.make_keyboard_search_group_aud(last_request=last_request,
                                                                                         page=page,
                                                                                         more_than_10=False,
                                                                                         requests=request_aud))
            bot.register_next_step_handler(msg, search_aud, bot=bot, storage=storage, tz=tz, last_msg=msg)

    if all_results and aud_list[chat_id] == []:
        all_found_aud = all_results
        request_word = data
        list_search = [page, request_word, all_found_aud]
        aud_list[chat_id] = list_search
        kb_all_results = keyboards.make_inline_keyboard_from_items(all_found_aud, items_in_row=3)

        msg = bot.send_message(
            chat_id=chat_id, reply_markup=kb_all_results,
            text="Результат поиска")

        bot.register_next_step_handler(msg, search_aud, bot=bot, storage=storage, tz=tz, last_msg=msg)



    else:
        if len(aud_list[chat_id]) == 3:
            pass
        elif all_results:
            kb_all_results = keyboards.make_inline_keyboard_from_items(all_found_aud, items_in_row=3)
            bot.send_message(
                chat_id=chat_id, reply_markup=kb_all_results,
                text="Результат поиска")
        else:
            msg = bot.send_message(chat_id=chat_id, text='Проверьте правильность ввода 😕',
                                   reply_markup=keyboards.make_keyboard_main_menu())
            bot.register_next_step_handler(msg, search_aud, bot=bot, storage=storage, tz=tz, last_msg=msg)
            return

    return


def handler_buttons_aud(bot, message, storage, tz):
    """Обрабатываем колбэк преподавателя"""
    global aud_list

    chat_id = message.message.chat.id
    message_id = message.message.message_id
    data = json.loads(message.data)
    all_found_aud = []

    if data['menu_aud'] == 'main':
        bot.send_message(chat_id=chat_id, text='Вы покинули поиск',
                         reply_markup=keyboards.make_keyboard_start_menu())
        bot.delete_message(chat_id, message_id)
        bot.clear_step_handler_by_chat_id(chat_id=chat_id)

        return

    if not aud_list[chat_id] and len(aud_list[chat_id]) != 0:
        aud_list[chat_id][1] = ''

    page = aud_list[chat_id][0]

    request_aud = storage.get_schedule_aud(aud_list[chat_id][1])
    # Циклы нужны для общего поиска. Здесь мы удаляем старые ключи в обоих реквестах и создаём один общий ключ, как для групп, так и для преподов
    for i in request_aud:
        i['search_aud'] = i.pop('aud')
    # Записываем слово, которое ищем
    request_word = aud_list[chat_id][1]
    last_request = request_aud[-1]
    # Эти циклы записывают группы и преподов в нижнем регистре для удобной работы с ними
    for i in request_aud:
        all_found_aud.append(i['search_aud'].lower())
    # Формируем полный багаж для пользователя
    list_search = [page, request_word, all_found_aud]
    # Записываем все данные под ключом пользователя
    aud_list[chat_id] = list_search

    if data['menu_aud'].lower() in aud_list[chat_id][2]:
        bot.delete_message(message_id=message_id, chat_id=chat_id)
        aud_list[chat_id][1] = data['menu_aud'].lower()
        des = message.data.split(":")[1].replace("}", "").replace('"', '')
        msg = bot.send_message(chat_id=chat_id,
                               text=f'Выберите неделю для аудитории{des}',
                               reply_markup=keyboards.make_keyboard_choose_schedule_for_aud_search())
        bot.register_next_step_handler(msg, choose_week, bot=bot, storage=storage, tz=tz, last_msg=msg)



    elif data['menu_aud'] == 'back':
        more_than_10 = False
        if len(request_aud) > 10:
            requests = request_aud[10 * (page - 1):10 * page]
            more_than_10 = True

        if aud_list[chat_id][0] - 1 == 0:
            bot.delete_message(message_id=message_id, chat_id=chat_id)
            bot.send_message(chat_id=chat_id, text=f'Результат поиска',
                             reply_markup=keyboards.make_keyboard_search_group_aud(last_request=last_request,
                                                                                   page=page - 1,
                                                                                   requests=requests,
                                                                                   more_than_10=more_than_10))

        else:
            bot.edit_message_reply_markup(message_id=message_id, chat_id=chat_id,
                                          reply_markup=keyboards.make_keyboard_search_group_aud(
                                              last_request=last_request,
                                              page=page - 1,
                                              requests=requests,
                                              more_than_10=more_than_10))
        aud_list[chat_id][0] -= 1

    elif data['menu_aud'] == 'next':
        bot.delete_message(message_id=message_id, chat_id=chat_id)
        more_than_10 = False
        if len(request_aud) > 10:
            requests = request_aud[10 * (page + 1):10 * (page + 2)]
            more_than_10 = True
        bot.send_message(chat_id=chat_id, text=f'Результат поиска',
                         reply_markup=keyboards.make_keyboard_search_group_aud(last_request=last_request,
                                                                               page=page + 1,
                                                                               requests=requests,
                                                                               more_than_10=more_than_10))
        aud_list[chat_id][0] += 1



    else:
        msg = bot.send_message(chat_id=chat_id, text='Проверьте правильность ввода 😞',
                               reply_markup=keyboards.make_keyboard_main_menu())
        bot.register_next_step_handler(msg, search_aud, bot=bot, storage=storage, tz=tz, last_msg=msg)


def choose_week(message, bot, storage, tz, last_msg=None):
    global aud_list
    chat_id = message.chat.id
    message = message.text

    if ('На текущую неделю' == message or 'На следующую неделю' == message):
        request_word = aud_list[chat_id][1]

        request_aud = storage.get_schedule_aud(request_word)

        # Если есть запрос для группы, то формируем расписание для группы, а если нет, то для препода
        schedule = request_aud[0]

        if not schedule:
            schedule_processing.sending_schedule_is_not_available_search(message=message, chat_id=chat_id, bot=bot)
            return

        schedule = schedule['schedule']
        week = find_week()

        # меняем неделю
        if message == 'На следующую неделю':
            week = 'odd' if week == 'even' else 'even'

        week_name = 'четная' if week == 'odd' else 'нечетная'
        aud = request_word
        schedule_str = full_schedule_in_str_prep(schedule, week=week, aud=aud)

        # Проверяем, что расписание сформировалось
        if isinstance(schedule_str, APIError):
            schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        bot.send_message(chat_id=chat_id, text=f'Расписание {request_word}\n'
                                               f'Неделя: {week_name}',
                         reply_markup=keyboards.make_keyboard_start_menu())

        # Отправка расписания
        schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_str)

        bot.clear_step_handler_by_chat_id(chat_id=chat_id)


def handler_buttons_aud_all_results(bot, message, storage, tz):
    chat_id = message.message.chat.id
    message_id = message.message.message_id
    data = message.data

    if data.lower() in aud_list[chat_id][2]:
        bot.delete_message(message_id=message_id, chat_id=chat_id)
        aud_list[chat_id][1] = data.lower()
        msg = bot.send_message(chat_id=chat_id,
                               text=f'Выберите неделю для аудитории{data}',
                               reply_markup=keyboards.make_keyboard_choose_schedule())
        bot.register_next_step_handler(msg, choose_week, bot=bot, storage=storage, tz=tz, last_msg=msg)

    else:
        return
