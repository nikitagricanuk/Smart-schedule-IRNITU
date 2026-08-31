from aiogram.exceptions import TelegramBadRequest

from tools import keyboards, statistics
from tools.logger import logger


async def _send_registration_data_loading_message(bot, chat_id):
    await bot.send_message(
        chat_id=chat_id,
        text='Расписание и списки групп еще загружаются ⏳\n'
        'Попробуйте снова через 1-2 минуты.',
    )


async def start(bot, message, storage, tz):
    """Команда бота Начать"""
    chat_id = message.chat.id

    # Проверяем есть пользователь в базе данных
    if await storage.get_user(chat_id):
        await storage.delete_user_or_userdata(chat_id)  # Удаляем пользвателя из базы данных

    institutes = await storage.get_institutes()
    if not institutes:
        logger.warning('Start requested while institutes are not loaded yet. chat_id=%s', chat_id)
        await _send_registration_data_loading_message(bot=bot, chat_id=chat_id)
        await statistics.add(action='start_loading', storage=storage, tz=tz)
        return

    logger.info('Sending /start registration keyboard. chat_id=%s institutes=%s', chat_id, len(institutes))
    try:
        await bot.send_message(
            chat_id=chat_id,
            text='Для начала пройдите небольшую регистрацию😉\nВыберите институт',
            reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
        )
    except TelegramBadRequest:
        logger.exception('Failed to send registration keyboard on /start. chat_id=%s', chat_id)
        await _send_registration_data_loading_message(bot=bot, chat_id=chat_id)
        return

    await statistics.add(action='start', storage=storage, tz=tz)


async def registration(bot, message, storage, tz):
    """Команда бота Регистрация"""
    chat_id = message.chat.id
    await storage.delete_user_or_userdata(chat_id=chat_id)
    institutes = await storage.get_institutes()
    if not institutes:
        logger.warning('Registration requested while institutes are not loaded yet. chat_id=%s', chat_id)
        await _send_registration_data_loading_message(bot=bot, chat_id=chat_id)
        await statistics.add(action='reg_loading', storage=storage, tz=tz)
        return

    logger.info('Sending /reg registration keyboard. chat_id=%s institutes=%s', chat_id, len(institutes))
    try:
        await bot.send_message(
            chat_id=chat_id,
            text='Пройдите повторную регистрацию😉\nВыберите институт',
            reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
        )
    except TelegramBadRequest:
        logger.exception('Failed to send registration keyboard on /reg. chat_id=%s', chat_id)
        await _send_registration_data_loading_message(bot=bot, chat_id=chat_id)
        return

    await statistics.add(action='reg', storage=storage, tz=tz)


async def show_map(bot, message, storage, tz):
    """Команда бота Карта"""
    chat_id = message.chat.id
    with open('map.jpg', "rb") as image:
        await bot.send_photo(chat_id, image)
    await statistics.add(action='map', storage=storage, tz=tz)


async def authors(bot, message, storage, tz):
    """Команда бота Авторы"""
    chat_id = message.chat.id
    await bot.send_message(
        chat_id=chat_id,
        parse_mode='HTML',
        text='<b>Авторы проекта:\n</b>'
        '- Алексей @bolanebyla\n'
        '- Султан @ace_sultan\n'
        '- Александр @alexandrshen\n'
        '- Владислав @TixoNNNAN\n'
        '- Кирилл @ADAMYORT\n\n'
        'По всем вопросом и предложениям пишите нам в личные сообщения. Будем рады 😉\n',
    )
    await statistics.add(action='authors', storage=storage, tz=tz)


async def tip(bot, message, storage, tz):
    """Команда бота Подсказка"""
    chat_id = message.chat.id
    await bot.send_message(
        chat_id=chat_id,
        parse_mode='HTML',
        text='Здравствуйте! Раз Вы вызвали Подсказку еще раз, значит дело серьезное.😬\n\n'
        'Напомню Вам основные советы по использованию бота:\n'
        '⏭Используйте кнопки, так я буду Вас лучше понимать!\n\n'
        '🌄Подгружайте расписание утром и оно будет в нашем чате до скончания времен!\n\n'
        '📃Чтобы просмотреть список доступных команд и кнопок, напишите в чате [Помощь]\n\n'
        '🆘Чтобы вызвать эту подсказку снова, напиши в чат [Подсказка] \n\n'
        'Если Вы столкнулись с технической проблемой, то Вы можете:\n'
        '- обратиться за помощью в официальную группу ВКонтакте [https://vk.com/smartschedule]\n'
        '- написать одному из моих создателей (команда Авторы)🤭\n',
    )
    await statistics.add(action='tip', storage=storage, tz=tz)


async def help_info(bot, message, storage, tz):
    chat_id = message.chat.id
    await bot.send_message(
        chat_id=chat_id,
        text='Список доступных Вам команд, для использования просто напишите их в чат😉:\n'
        '/start – запустить диалог с ботом сначала\n'
        '/reg – пройти регистрацию заново\n'
        '/map – карта корпусов ИРНИТУ\n'
        '/about – краткая информация о боте\n'
        '/authors – мои создатели\n'
        '/tip – подсказка\n'
        '/help – список доступных команд\n',
    )

    await statistics.add(action='help', storage=storage, tz=tz)


async def about(bot, message, storage, tz):
    chat_id = message.chat.id
    await bot.send_message(
        chat_id=chat_id,
        parse_mode='HTML',
        text='<b>О боте:\n</b>'
        'Smart schedule IRNITU bot - это чат бот для просмотра расписания занятий в '
        'Иркутском национальном исследовательском техническом университете\n\n'
        '<b>Благодаря боту можно:\n</b>'
        '- Узнать актуальное расписание\n'
        '- Нажатием одной кнопки увидеть информацию о ближайшей паре\n'
        '- Настроить гибкие уведомления с информацией из расписания, '
        'которые будут приходить за определённое время до начала занятия',
    )

    await statistics.add(action='about', storage=storage, tz=tz)
