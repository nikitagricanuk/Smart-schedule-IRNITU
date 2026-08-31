from tools.get_text_schedule_not_available import get_text_schedule_not_available
from tools import keyboards


async def check_schedule(bot, chat_id, schedule) -> bool:
    """Проверяем есть ли у группы расписание"""
    if not schedule:
        await bot.send_message(
            chat_id=chat_id,
            text=get_text_schedule_not_available(),
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        return False
    if not schedule['schedule']:
        await bot.send_message(
            chat_id=chat_id,
            text=get_text_schedule_not_available(),
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        return False

    return True
