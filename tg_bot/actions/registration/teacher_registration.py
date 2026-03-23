import json
from tools import keyboards


def start_prep_reg(bot, message, storage):
    """Вхождение в стейт регистрации преподавателей"""

    chat_id = message.message.chat.id
    message_id = message.message.message_id
    data = message.data

    # После того как пользователь выбрал институт
    if 'institute' in data:
        data = json.loads(data)

        storage.save_or_update_user(chat_id=chat_id,
                                    institute=data['institute'],
                                    course='None')  # Записываем в базу институт пользователя

        # Выводим сообщение со списком курсов
        bot.send_message(chat_id, text='📚Кто постигает новое, лелея старое,\n'
                                       'Тот может быть учителем.\n'
                                       'Конфуций')

        msg = bot.send_message(chat_id, text='Введите своё ФИО полностью.\n'
                                             'Например: Корняков Михаил Викторович')
        bot.register_next_step_handler(msg, reg_prep_step_2, bot, storage)
        bot.delete_message(message_id=message_id, chat_id=chat_id)

        return


def reg_prep_step_2(message, bot, storage, last_msg=None):
    """Регистрация преподавателя"""

    chat_id = message.chat.id
    message = message.text
    user = storage.get_user(chat_id)

    if not user:
        return

    if last_msg:
        message_id = last_msg.message_id
        bot.delete_message(message_id=message_id, chat_id=chat_id)

    prep_list = storage.get_prep(message)
    if prep_list:
        prep_name = prep_list[0]['prep']
        storage.save_or_update_user(chat_id=chat_id, group=prep_name)
        bot.send_message(chat_id, text=f'Вы успешно зарегистрировались, как {prep_name}!😊\n\n'
                                       'Для того чтобы пройти регистрацию повторно, напишите сообщение "Регистрация"\n',
                         reply_markup=keyboards.make_keyboard_start_menu())
        return

    elif not prep_list:
        # Делим введенное фио на части и ищем по каждой в базе
        prep_list = []
        prep_list_2 = []
        prep_and_id_list = []
        content_commands = ['Начать', 'начать', 'Начало', 'start', '/start', 'Регистрация', '/reg']
        matches_by_word = {}

        # Делим полученное ФИО на отдельные слова, на выходе имеем второй список с уникальными значениями по запросу
        for name_unit in message.split():
            # Ищем в базе преподов по каждому слову
            for i in storage.get_register_list_prep(name_unit):
                prep_and_id_list.append(i)
                prep_list.append(i['prep'])
            matches_by_word[name_unit] = list(set(prep_list))
            # Если 2 списка не пустых, ищем элементы, которые повторяются максимальное количество раз
            if prep_list and prep_list_2:
                prep_list_2 = list(set(prep_list) & set(prep_list_2))
            # Если второй список пуст (еще остались слова из запроса, по которым не сходили в базу)
            elif prep_list and not prep_list_2:
                prep_list_2 = prep_list
            prep_list = []

        # На сайте ИРНИТУ преподаватели часто хранятся как "Фамилия И.О.".
        # Для запроса вида "Фамилия Имя Отчество" оставляем fallback по фамилии.
        if not prep_list_2 and message.split():
            surname = message.split()[0]
            prep_list_2 = matches_by_word.get(surname, [])

        # Ограничивает размер клавы до 20 преподов
        if len(prep_list_2) > 20:
            prep_list_2 = prep_list_2[:20]
        # Создается сортированный список со словарями из коллекции prepods_schedule
        sort_prep = []
        # Если ФИО преподаывателя содержится в prep_list_2, то записываем его словарь (из коллекции prepods_schedule)
        # в новый список sort_prep
        for i in range(len(prep_and_id_list)):
            if prep_and_id_list[i]['prep'] in prep_list_2:
                sort_prep.append(prep_and_id_list[i])
        # Если sort_prep не пустой, выдаем клавиатуру с возможными вариантами
        if sort_prep:
            # Сообщение на которое пользователь отвечает (сохраняет сообщение, введенное пользователем,
            # после фразы 'Возможно вы имелли в виду:')
            msg = bot.send_message(chat_id=chat_id, text=f'Возможно вы имелли в виду:',
                                   reply_markup=keyboards.make_inline_keyboard_reg_prep(sort_prep))
            bot.register_next_step_handler(msg, reg_prep_step_2, bot, storage, last_msg=msg)
        # Если sort_prep пустой и сообщение содержит в себе попытку перерегистрации
        elif message in content_commands:
            bot.send_message(chat_id=chat_id, text='Выберите институт',
                             reply_markup=keyboards.make_inline_keyboard_choose_institute(storage.get_institutes()))
            return
        # Если sort_prep пустой, то выводим ошибку
        else:
            msg = bot.send_message(chat_id=chat_id, text='Проверьте правильность ввода 😞')
            bot.register_next_step_handler(msg, reg_prep_step_2, bot, storage)
    return


def reg_prep_choose_from_list(bot, message, storage):
    """Обрабатываем колбэк преподавателя"""

    chat_id = message.message.chat.id
    message_id = message.message.message_id
    data = json.loads(message.data)

    # Выходим из цикла поиска преподавателя по ФИО
    bot.clear_step_handler_by_chat_id(chat_id=chat_id)

    # Назад к институтам
    if data['prep_id'] == 'back':
        bot.send_message(chat_id=chat_id, text='Выберите институт',
                         reply_markup=keyboards.make_inline_keyboard_choose_institute(storage.get_institutes()))
        storage.delete_user_or_userdata(chat_id)
    # Регистрируем преподавателя по выбранной кнопке
    else:
        prep_name = storage.get_prep_for_id(data['prep_id'])['prep']
        storage.save_or_update_user(chat_id=chat_id, group=prep_name)
        bot.delete_message(message_id=message_id, chat_id=chat_id)
        bot.send_message(chat_id, text=f'Приветствую Вас, Пользователь! Вы успешно зарегистрировались, как {prep_name}!😊\n\n'
                                       "Я чат-бот для просмотра расписания занятий в Иркутском Политехе.🤖\n\n"
                                        "С помощью меня можно не только смотреть свое расписание на день или неделю, но и осуществлять поиск расписания по группам, аудиториям и преподавателям (кнопка [Поиск]).\n"
                                        "А еще можно настроить уведомления о парах (в разделе [Другое] кнопка [Напоминания]).\n\n"
                                        "Следующие советы помогут раскрыть мой функционал на 💯 процентов:\n"
                                        "⏭Используйте кнопки, так я буду Вас лучше понимать!\n\n"
                                        "🌄Подгружайте расписание утром и оно будет в нашем чате до скончания времен!\n\n"
                                        "📃Чтобы просмотреть список доступных команд и кнопок, напишите в чате [Помощь]\n\n"
                                        "🆘Чтобы вызвать эту подсказку снова, напиши в чат [Подсказка] \n\n"
                                        "Надеюсь, что Вам будет удобно меня использовать. Для того чтобы пройти регистрацию повторно, напишите сообщение [Регистрация]\n\n"
                                        "Если Вы столкнетесь с технической проблемой, то Вы можете:\n"
                                        "- обратиться за помощью в официальную группу ВКонтакте [https://vk.com/smartschedule]\n"
                                        "- написать одному из моих создателей (команда Авторы)🤭\n",
                         reply_markup=keyboards.make_keyboard_start_menu())
