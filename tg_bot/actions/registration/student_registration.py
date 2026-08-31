import json
from tools.logger import logger
from tools import keyboards


def _parse_registration_callback(data):
    if not data:
        return None, None

    if data.startswith('inst:'):
        return 'institute', data.split(':', 1)[1]
    if data.startswith('course:'):
        return 'course', data.split(':', 1)[1]
    if data.startswith('group:'):
        return 'group', data.split(':', 1)[1]

    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None, None

    for key in ('institute', 'course', 'group'):
        if key in payload:
            return key, payload[key]
    return None, None


async def start_student_reg(bot, callback, storage):
    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    key, value = _parse_registration_callback(callback.data)

    # После того как пользователь выбрал институт
    if key == 'institute':
        institute_name = value
        institute_doc = await storage.get_institute_by_id(value)
        if institute_doc:
            institute_name = institute_doc.get('name')

        if not institute_name:
            await bot.send_message(chat_id=chat_id, text='Не удалось определить институт. Нажмите /start и повторите.')
            return

        courses = await storage.get_courses(institute_name)

        await storage.save_or_update_user(chat_id=chat_id, institute=institute_name)
        try:
            # Выводим сообщение со списком курсов
            await bot.edit_message_text(
                message_id=message_id,
                chat_id=chat_id,
                text='Выберите курс',
                reply_markup=keyboards.make_inline_keyboard_choose_courses(courses),
            )
        except Exception as e:
            logger.exception(e)
            return

    # После того как пользователь выбрал курс или нажал кнопку назад при выборе курса
    elif key == 'course':
        course_name = value

        # Если нажали кнопку назад
        if course_name == 'back':
            await storage.delete_user_or_userdata(chat_id=chat_id)
            institutes = await storage.get_institutes()
            try:
                await bot.edit_message_text(
                    message_id=message_id,
                    chat_id=chat_id,
                    text='Выберите институт',
                    reply_markup=keyboards.make_inline_keyboard_choose_institute(institutes),
                )
                return
            except Exception as e:
                logger.exception(e)
                return

        course_doc = await storage.get_course_by_id(value)
        if course_doc:
            course_name = course_doc.get('name')

        if not course_name:
            await bot.send_message(chat_id=chat_id, text='Не удалось определить курс. Нажмите /start и повторите.')
            return

        await storage.save_or_update_user(chat_id=chat_id, course=course_name)
        user = await storage.get_user(chat_id=chat_id)

        try:
            institute = user['institute']
            course = user['course']
            groups = await storage.get_groups(institute=institute, course=course)
            # Выводим сообщение со списком групп
            await bot.edit_message_text(
                message_id=message_id,
                chat_id=chat_id,
                text='Выберите группу',
                reply_markup=keyboards.make_inline_keyboard_choose_groups(groups),
            )
        except Exception as e:
            logger.exception(e)
            return

    # После того как пользователь выбрал группу или нажал кнопку назад при выборе группы
    elif key == 'group':
        group_name = value

        # Если нажали кнопку назад
        if group_name == 'back':
            # Удаляем информацию о курсе пользователя из базы данных
            await storage.delete_user_or_userdata(chat_id=chat_id, delete_only_course=True)
            try:
                institute = (await storage.get_user(chat_id=chat_id))['institute']
            except Exception as e:
                logger.exception(e)
                return
            courses = await storage.get_courses(institute=institute)

            try:
                # Выводим сообщение со списком курсов
                await bot.edit_message_text(
                    message_id=message_id,
                    chat_id=chat_id,
                    text='Выберите курс',
                    reply_markup=keyboards.make_inline_keyboard_choose_courses(courses),
                )
                return
            except Exception as e:
                logger.exception(e)
                return

        group_doc = await storage.get_group_by_id(value)
        if group_doc:
            group_name = group_doc.get('name')

        if not group_name:
            await bot.send_message(chat_id=chat_id, text='Не удалось определить группу. Нажмите /start и повторите.')
            return

        await storage.save_or_update_user(chat_id=chat_id, group=group_name)

        try:
            # Удаляем меню регистрации
            await bot.delete_message(message_id=message_id, chat_id=chat_id)
        except Exception as e:
            logger.exception(e)
            return

        await bot.send_message(
            chat_id=chat_id,
            text="Приветствую Вас, Пользователь! Вы успешно зарегистрировались!😊 \n\n"
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
    else:
        logger.warning('Unknown registration callback payload. chat_id=%s data=%s', chat_id, callback.data)
