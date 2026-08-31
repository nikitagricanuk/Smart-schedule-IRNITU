from tools import keyboards


async def sending_schedule(bot, chat_id, schedule_str: str):
    """Отправка расписания пользователю"""
    for schedule in schedule_str:
        await bot.send_message(
            chat_id=chat_id,
            text=f'{schedule}',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )


async def sending_schedule_is_not_available(bot, chat_id):
    await bot.send_message(
        chat_id=chat_id,
        text='Расписание временно недоступно🚫😣\nПопробуйте позже⏱',
        reply_markup=keyboards.make_keyboard_start_menu(),
    )


async def sending_service_is_not_available(bot, chat_id):
    await bot.send_message(
        chat_id=chat_id,
        text='Сервис временно недоступен🚫😣\nПопробуйте позже⏱',
        reply_markup=keyboards.make_keyboard_start_menu(),
    )
