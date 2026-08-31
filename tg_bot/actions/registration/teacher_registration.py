import json

from tools import keyboards

WAITING_TEACHER_NAME = set()


def _parse_teacher_callback(data):
    if data == 'inst:teacher':
        return 'Преподаватель'
    if data and data.startswith('inst:'):
        return data.split(':', 1)[1]

    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None

    return payload.get('institute')


async def start_prep_reg(bot, callback, storage):
    """Вхождение в регистрацию преподавателей."""
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    institute = _parse_teacher_callback(callback.data)
    if not institute:
        return

    if institute != 'Преподаватель':
        institute_doc = await storage.get_institute_by_id(institute)
        if institute_doc:
            institute = institute_doc.get('name')

    await storage.save_or_update_user(
        chat_id=chat_id,
        institute=institute,
        course='None',
    )

    await bot.send_message(
        chat_id,
        text='📚Кто постигает новое, лелея старое,\nТот может быть учителем.\nКонфуций',
    )
    await bot.send_message(chat_id, text='Введите своё ФИО полностью.\nНапример: Корняков Михаил Викторович')
    try:
        await bot.delete_message(message_id=message_id, chat_id=chat_id)
    except Exception:
        pass
    WAITING_TEACHER_NAME.add(chat_id)


async def process_teacher_name_input(bot, message, storage) -> bool:
    chat_id = message.chat.id
    if chat_id not in WAITING_TEACHER_NAME:
        return False

    user = await storage.get_user(chat_id)
    if not user:
        WAITING_TEACHER_NAME.discard(chat_id)
        return True

    incoming_text = (message.text or '').strip()
    prep_list = await storage.get_prep(incoming_text)
    if prep_list:
        prep_name = prep_list[0]['prep']
        await storage.save_or_update_user(chat_id=chat_id, group=prep_name)
        await bot.send_message(
            chat_id,
            text=f'Вы успешно зарегистрировались, как {prep_name}!😊\n\n'
            'Для того чтобы пройти регистрацию повторно, напишите сообщение "Регистрация"\n',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        WAITING_TEACHER_NAME.discard(chat_id)
        return True

    content_commands = {'Начать', 'начать', 'Начало', 'start', '/start', 'Регистрация', '/reg'}
    if incoming_text in content_commands:
        institutes = await storage.get_institutes()
        if not institutes:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание и списки групп еще загружаются ⏳\nПопробуйте снова через 1-2 минуты.',
            )
            WAITING_TEACHER_NAME.discard(chat_id)
            return True
        await bot.send_message(
            chat_id=chat_id,
            text='Выберите институт',
            reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
        )
        WAITING_TEACHER_NAME.discard(chat_id)
        return True

    prep_list_exact = []
    prep_and_id_list = []
    matches_by_word = {}
    for name_unit in incoming_text.split():
        per_word = await storage.get_register_list_prep(name_unit)
        current_word_matches = set()
        for prep in per_word:
            prep_and_id_list.append(prep)
            prep_list_exact.append(prep['prep'])
            current_word_matches.add(prep['prep'])
        matches_by_word[name_unit] = list(current_word_matches)

    prep_list_2 = []
    for values in matches_by_word.values():
        if values and prep_list_2:
            prep_list_2 = list(set(values) & set(prep_list_2))
        elif values and not prep_list_2:
            prep_list_2 = values

    if not prep_list_2 and incoming_text.split():
        surname = incoming_text.split()[0]
        prep_list_2 = matches_by_word.get(surname, [])

    if len(prep_list_2) > 20:
        prep_list_2 = prep_list_2[:20]

    sort_prep = [item for item in prep_and_id_list if item['prep'] in prep_list_2]
    if sort_prep:
        await bot.send_message(
            chat_id=chat_id,
            text='Возможно вы имелли в виду:',
            reply_markup=keyboards.make_inline_keyboard_reg_prep(sort_prep),
        )
        return True

    await bot.send_message(chat_id=chat_id, text='Проверьте правильность ввода 😞')
    return True


async def reg_prep_choose_from_list(bot, callback, storage):
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    data = json.loads(callback.data)

    WAITING_TEACHER_NAME.discard(chat_id)

    if data['prep_id'] == 'back':
        institutes = await storage.get_institutes()
        if not institutes:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание и списки групп еще загружаются ⏳\nПопробуйте снова через 1-2 минуты.',
            )
            await storage.delete_user_or_userdata(chat_id)
            return
        await bot.send_message(
            chat_id=chat_id,
            text='Выберите институт',
            reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
        )
        await storage.delete_user_or_userdata(chat_id)
        return

    prep_doc = await storage.get_prep_for_id(data['prep_id'])
    if not prep_doc:
        await bot.send_message(chat_id=chat_id, text='Преподаватель не найден. Попробуйте снова.')
        return

    prep_name = prep_doc['prep']
    await storage.save_or_update_user(chat_id=chat_id, group=prep_name)
    try:
        await bot.delete_message(message_id=message_id, chat_id=chat_id)
    except Exception:
        pass
    await bot.send_message(
        chat_id,
        text=f'Приветствую Вас, Пользователь! Вы успешно зарегистрировались, как {prep_name}!😊\n\n'
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
        reply_markup=keyboards.make_keyboard_start_menu(),
    )
