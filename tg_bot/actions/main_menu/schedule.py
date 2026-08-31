from datetime import datetime

from API.functions_api import APIError, find_week
from API.functions_api import (
    full_schedule_in_str,
    full_schedule_in_str_prep,
    get_near_lesson,
    get_next_day_schedule_in_str,
    get_next_day_schedule_in_str_prep,
    get_now_lesson,
    get_now_lesson_in_str_prep,
    get_now_lesson_in_str_stud,
    get_one_day_schedule_in_str,
    get_one_day_schedule_in_str_prep,
    schedule_view_exams,
)
from tools import keyboards, schedule_processing, statistics


async def groups_exam(group, storage):
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


async def _get_user_schedule(storage, chat_id):
    user = await storage.get_user(chat_id=chat_id)
    if user['course'] != 'None':
        group = user['group']
        schedule = await storage.get_schedule(group=group)
    else:
        group = user['group']
        schedule = await storage.get_schedule_prep(group=group)
    return user, group, schedule


async def get_schedule(bot, message, storage, tz):
    chat_id = message.chat.id
    data = message.text
    user = await storage.get_user(chat_id=chat_id)

    if 'Расписание 🗓' == data and user and user.get('group'):
        await bot.send_message(
            chat_id=chat_id,
            text='Выберите период\n',
            reply_markup=keyboards.make_keyboard_choose_schedule(),
        )
        await statistics.add(action='Расписание', storage=storage, tz=tz)
        return

    if ('На текущую неделю' == data or 'На следующую неделю' == data) and user and user.get('group'):
        user, group, schedule_doc = await _get_user_schedule(storage, chat_id)
        if not schedule_doc or schedule_doc.get('schedule') == []:
            await bot.send_message(chat_id=chat_id, text='Расписание временно недоступно\nПопробуйте позже⏱')
            await statistics.add(action=data, storage=storage, tz=tz)
            return

        schedule = schedule_doc['schedule']
        week = await find_week()
        if data == 'На следующую неделю':
            week = 'odd' if week == 'even' else 'even'

        week_name = 'четная' if week == 'odd' else 'нечетная'
        if user['course'] != 'None':
            schedule_str = await full_schedule_in_str(schedule, week=week)
        else:
            schedule_str = await full_schedule_in_str_prep(schedule, week=week)

        if isinstance(schedule_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        await bot.send_message(
            chat_id=chat_id,
            text=f'Расписание {group}\nНеделя: {week_name}',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        await schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_str)
        await statistics.add(action=data, storage=storage, tz=tz)
        return

    if 'Расписание на сегодня 🍏' == data and user and user.get('group'):
        user, _, schedule_doc = await _get_user_schedule(storage, chat_id)
        if not schedule_doc:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание временно недоступно🚫😣\nПопробуйте позже⏱',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Расписание на сегодня', storage=storage, tz=tz)
            return

        schedule = schedule_doc['schedule']
        week = await find_week()
        if user['course'] != 'None':
            schedule_one_day = await get_one_day_schedule_in_str(schedule=schedule, week=week)
        else:
            schedule_one_day = await get_one_day_schedule_in_str_prep(schedule=schedule, week=week)

        if isinstance(schedule_one_day, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        if not schedule_one_day:
            await bot.send_message(chat_id=chat_id, text='Сегодня пар нет 😎')
            return
        await bot.send_message(chat_id=chat_id, text=f'{schedule_one_day}')
        await statistics.add(action='Расписание на сегодня', storage=storage, tz=tz)
        return

    if 'Экзамены' in data and user and user.get('group'):
        group = user['group']
        schedule = await groups_exam(group=group, storage=storage)
        if not schedule:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание экзаменов отсутствует😇\nПопробуйте позже⏱',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Экзамены', storage=storage, tz=tz)
            return

        schedule_exams = await schedule_view_exams(schedule=schedule)
        if isinstance(schedule_exams, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        await schedule_processing.sending_schedule(bot=bot, chat_id=chat_id, schedule_str=schedule_exams)
        await statistics.add(action='Экзамены', storage=storage, tz=tz)
        return

    if 'Расписание на завтра 🍎' == data and user and user.get('group'):
        user, _, schedule_doc = await _get_user_schedule(storage, chat_id)
        if not schedule_doc:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание временно недоступно🚫😣\nПопробуйте позже⏱',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Расписание на завтра', storage=storage, tz=tz)
            return
        schedule = schedule_doc['schedule']
        week = await find_week()
        if datetime.today().isoweekday() == 7:
            if week == 'odd':
                week = 'even'
            elif week == 'even':
                week = 'odd'
            else:
                week = 'all'

        if user['course'] != 'None':
            schedule_next_day = await get_next_day_schedule_in_str(schedule=schedule, week=week)
        else:
            schedule_next_day = await get_next_day_schedule_in_str_prep(schedule=schedule, week=week)

        if isinstance(schedule_next_day, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        if not schedule_next_day:
            await bot.send_message(chat_id=chat_id, text='Завтра пар нет 😎')
            return
        await bot.send_message(chat_id=chat_id, text=f'{schedule_next_day}')
        await statistics.add(action='Расписание на завтра', storage=storage, tz=tz)
        return

    if 'Ближайшая пара ⏱' in data and user and user.get('group'):
        await bot.send_message(
            chat_id=chat_id,
            text='Ближайшая пара',
            reply_markup=keyboards.make_keyboard_nearlesson(),
        )
        await statistics.add(action='Ближайшая пара', storage=storage, tz=tz)
        return

    if 'Текущая' in data and user and user.get('group'):
        user, _, schedule_doc = await _get_user_schedule(storage, chat_id)
        if not schedule_doc:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание временно недоступно🚫😣\nПопробуйте позже⏱',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Текущая', storage=storage, tz=tz)
            return
        schedule = schedule_doc['schedule']
        week = await find_week()
        now_lessons = await get_now_lesson(schedule=schedule, week=week)
        if isinstance(now_lessons, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return
        if not now_lessons:
            await bot.send_message(
                chat_id=chat_id,
                text='Сейчас пары нет, можете отдохнуть)',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Текущая', storage=storage, tz=tz)
            return

        if user['course'] != 'None':
            now_lessons_str = await get_now_lesson_in_str_stud(now_lessons)
        else:
            now_lessons_str = await get_now_lesson_in_str_prep(now_lessons)

        if isinstance(now_lessons_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        await bot.send_message(
            chat_id=chat_id,
            text=f'🧠Текущая пара🧠\n{now_lessons_str}',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        await statistics.add(action='Текущая', storage=storage, tz=tz)
        return

    if 'Следующая' in data and user and user.get('group'):
        user, _, schedule_doc = await _get_user_schedule(storage, chat_id)
        if not schedule_doc:
            await bot.send_message(
                chat_id=chat_id,
                text='Расписание временно недоступно🚫😣\nПопробуйте позже⏱',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Следующая', storage=storage, tz=tz)
            return
        schedule = schedule_doc['schedule']
        week = await find_week()
        near_lessons = await get_near_lesson(schedule=schedule, week=week)
        if isinstance(near_lessons, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return
        if not near_lessons:
            await bot.send_message(
                chat_id=chat_id,
                text='Сегодня больше пар нет 😎',
                reply_markup=keyboards.make_keyboard_start_menu(),
            )
            await statistics.add(action='Следующая', storage=storage, tz=tz)
            return

        if user['course'] != 'None':
            near_lessons_str = await get_now_lesson_in_str_stud(near_lessons)
        else:
            near_lessons_str = await get_now_lesson_in_str_prep(near_lessons)

        if isinstance(near_lessons_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(bot=bot, chat_id=chat_id)
            return

        await bot.send_message(
            chat_id=chat_id,
            text=f'🧠Ближайшая пара🧠\n{near_lessons_str}',
            reply_markup=keyboards.make_keyboard_start_menu(),
        )
        await statistics.add(action='Следующая', storage=storage, tz=tz)
