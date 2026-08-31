from tools import keyboards, statistics


async def processing_main_buttons(bot, message, storage, tz):
    chat_id = message.chat.id
    data = message.text
    user = await storage.get_user(chat_id=chat_id)

    if 'Основное меню' in data and user:
        await bot.send_message(chat_id, text='Основное меню', reply_markup=keyboards.make_keyboard_start_menu())

        await statistics.add(action='Основное меню', storage=storage, tz=tz)

    elif 'Список команд' in data and user:
        await bot.send_message(
            chat_id,
            text='Список команд:\n'
            'Авторы - список авторов \n'
            'Регистрация- повторная регистрация\n'
            'Карта - карта университета',
            reply_markup=keyboards.make_keyboard_commands(),
        )

        await statistics.add(action='Список команд', storage=storage, tz=tz)

    elif 'Другое ⚡' in data and user:
        await bot.send_message(chat_id, text='Другое', reply_markup=keyboards.make_keyboard_extra())

        await statistics.add(action='Другое', storage=storage, tz=tz)
