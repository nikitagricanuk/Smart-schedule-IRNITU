from datetime import datetime
from tools.storage import MongodbService
from vkbottle.bot import Message

from API.functions_api import find_week, full_schedule_in_str, full_schedule_in_str_prep, \
    get_one_day_schedule_in_str_prep, get_one_day_schedule_in_str, get_next_day_schedule_in_str, \
    get_next_day_schedule_in_str_prep, APIError, get_now_lesson_in_str_stud, get_now_lesson_in_str_prep,\
    schedule_view_exams
from API.functions_api import get_near_lesson, get_now_lesson
from tools import keyboards, statistics, schedule_processing

storage = MongodbService().get_instance()


def groups_exam(group):
    schedule = storage.get_schedule_exam(group=group)
    if not schedule:
        return 0
    del schedule['_id']
    clear_list = []
    for i in range(len(schedule['exams']['exams'])):
        if schedule['exams']['exams'][i] not in clear_list:
            clear_list.append(schedule['exams']['exams'][i])
    schedule['exams']['exams'] = clear_list
    return schedule


async def _send_schedule_not_published(ans: Message):
    await ans.answer(
        (
            "\u0414\u043b\u044f \u0432\u0430\u0448\u0435\u0439 \u0433\u0440\u0443\u043f\u043f\u044b "
            "\u0441\u0435\u0439\u0447\u0430\u0441 \u043d\u0435\u0442 "
            "\u043e\u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u043d\u043d\u043e\u0433\u043e "
            "\u0440\u0430\u0441\u043f\u0438\u0441\u0430\u043d\u0438\u044f \u043d\u0430 "
            "\u0441\u0430\u0439\u0442\u0435 \u0418\u0420\u041d\u0418\u0422\u0423\n"
            "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 "
            "\u043f\u043e\u0437\u0436\u0435\u23f1"
        ),
        keyboard=keyboards.make_keyboard_start_menu(),
    )


async def get_schedule(ans: Message, storage, tz):
    chat_id = ans.from_id
    data = ans.text
    user = storage.get_vk_user(chat_id=chat_id)

    if 'Расписание 🗓' == data and user.get('group'):
        await ans.answer('Выберите период\n', keyboard=keyboards.make_keyboard_choose_schedule())
        statistics.add(action='Расписание', storage=storage, tz=tz)

    if ('На текущую неделю' == data or 'На следующую неделю' == data) and user.get('group'):
        # Если курс нуль, тогда это преподаватель
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule_prep(group=group)
        if schedule and schedule.get('schedule') == []:
            await _send_schedule_not_published(ans=ans)
            statistics.add(action=data, storage=storage, tz=tz)
            return
        if not schedule or schedule['schedule'] == []:
            await ans.answer('Расписание временно недоступно\nПопробуйте позже⏱')
            statistics.add(action=data, storage=storage, tz=tz)
            return

        schedule = schedule['schedule']
        week = find_week()

        # меняем неделю
        if data == 'На следующую неделю':
            week = 'odd' if week == 'even' else 'even'

        week_name = 'нечетная' if week == 'odd' else 'четная'

        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            schedule_str = full_schedule_in_str(schedule, week=week)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            schedule_str = full_schedule_in_str_prep(schedule, week=week)

        # Проверяем, что расписание сформировалось
        if isinstance(schedule_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        await ans.answer(f'Расписание {group}\n'
                         f'Неделя: {week_name}', keyboard=keyboards.make_keyboard_start_menu())

        # Отправка расписания
        await schedule_processing.sending_schedule(ans=ans, schedule_str=schedule_str)

        statistics.add(action=data, storage=storage, tz=tz)



    elif 'Расписание на сегодня 🍏' == data and user.get('group'):
        # Если курс нуль, тогда это преподаватель
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule_prep(group=group)
        if schedule and schedule.get('schedule') == []:
            await _send_schedule_not_published(ans=ans)
            statistics.add(action='Р Р°СЃРїРёСЃР°РЅРёРµ РЅР° СЃРµРіРѕРґРЅСЏ', storage=storage, tz=tz)
            return
        if not schedule:
            await ans.answer('Расписание временно недоступно🚫😣\n'
                             'Попробуйте позже⏱', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Расписание на завтра', storage=storage, tz=tz)
            return
        schedule = schedule['schedule']
        week = find_week()
        # Если курс нуль, тогда это преподаватель
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            schedule_one_day = get_one_day_schedule_in_str(schedule=schedule, week=week)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            schedule_one_day = get_one_day_schedule_in_str_prep(schedule=schedule, week=week)

        # Проверяем, что расписание сформировалось
        if isinstance(schedule_one_day, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        if not schedule_one_day:
            await ans.answer('Сегодня пар нет 😎')
            return
        await ans.answer(f'{schedule_one_day}')
        statistics.add(action='Расписание на сегодня', storage=storage, tz=tz)

    elif 'Расписание на завтра 🍎' == data and user.get('group'):
        # Если курс нуль, тогда это преподаватель
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule_prep(group=group)
        if schedule and schedule.get('schedule') == []:
            await _send_schedule_not_published(ans=ans)
            statistics.add(action='Р Р°СЃРїРёСЃР°РЅРёРµ РЅР° Р·Р°РІС‚СЂР°', storage=storage, tz=tz)
            return
        if not schedule:
            await ans.answer('Расписание временно недоступно🚫😣\n'
                             'Попробуйте позже⏱', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Расписание на завтра', storage=storage, tz=tz)
            return
        schedule = schedule['schedule']
        week = find_week()
        if datetime.today().isoweekday() == 7:
            if week == 'odd':
                week = 'even'
            elif week == 'even':
                week = 'odd'
            else:
                week = 'all'

        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            schedule_next_day = get_next_day_schedule_in_str(schedule=schedule, week=week)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            schedule_next_day = get_next_day_schedule_in_str_prep(schedule=schedule, week=week)

        # Проверяем, что расписание сформировалось
        if isinstance(schedule_next_day, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        if not schedule_next_day:
            await ans.answer('Завтра пар нет 😎')
            return
        await ans.answer(f'{schedule_next_day}')
        statistics.add(action='Расписание на завтра', storage=storage, tz=tz)

    elif 'Ближайшая пара ⏱' in data and user.get('group'):
        await ans.answer('Ближайшая пара', keyboard=keyboards.make_keyboard_nearlesson())
        statistics.add(action='Ближайшая пара', storage=storage, tz=tz)
        return



    elif 'Экзамены' in data and user.get('group'):
        # Если курс нуль, тогда это преподаватель
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = groups_exam(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = groups_exam(group=group)

        if not schedule:
            await ans.answer('Расписание экзаменов отсутствует😇\n'
                             'Попробуйте позже⏱', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Экзамены', storage=storage, tz=tz)
            return

        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            schedule_exams = schedule_view_exams(schedule=schedule)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            schedule_exams = schedule_view_exams(schedule=schedule)

        # Проверяем, что расписание сформировалось
        if isinstance(schedule_exams, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        await schedule_processing.sending_schedule(ans=ans, schedule_str=schedule_exams)
        statistics.add(action='Экзамены', storage=storage, tz=tz)




    elif 'Текущая' in data and user.get('group'):
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule_prep(group=group)
        if schedule and schedule.get('schedule') == []:
            await _send_schedule_not_published(ans=ans)
            statistics.add(action='РўРµРєСѓС‰Р°СЏ', storage=storage, tz=tz)
            return
        if not schedule:
            await ans.answer('Расписание временно недоступно🚫😣\n'
                             'Попробуйте позже⏱', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Текущая', storage=storage, tz=tz)
            return
        schedule = schedule['schedule']
        week = find_week()

        now_lessons = get_now_lesson(schedule=schedule, week=week)

        # Проверяем, что расписание сформировалось
        if isinstance(now_lessons, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        # если пар нет
        if not now_lessons:
            await ans.answer('Сейчас пары нет, можете отдохнуть)', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Текущая', storage=storage, tz=tz)
            return

        # Студент
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            now_lessons_str = get_now_lesson_in_str_stud(now_lessons)

        # Преподаватель
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            now_lessons_str = get_now_lesson_in_str_prep(now_lessons)

        # Проверяем, что расписание сформировалось
        if isinstance(now_lessons_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        await ans.answer(f'🧠Текущая пара🧠\n'f'{now_lessons_str}', keyboard=keyboards.make_keyboard_start_menu())

        statistics.add(action='Текущая', storage=storage, tz=tz)

    elif 'Следующая' in data and user.get('group'):
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule(group=group)
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            group = storage.get_vk_user(chat_id=chat_id)['group']
            schedule = storage.get_schedule_prep(group=group)
        if schedule and schedule.get('schedule') == []:
            await _send_schedule_not_published(ans=ans)
            statistics.add(action='РЎР»РµРґСѓСЋС‰Р°СЏ', storage=storage, tz=tz)
            return
        if not schedule:
            await ans.answer('Расписание временно недоступно🚫😣\n'
                             'Попробуйте позже⏱', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Следующая', storage=storage, tz=tz)
            return
        schedule = schedule['schedule']
        week = find_week()

        near_lessons = get_near_lesson(schedule=schedule, week=week)

        # Проверяем, что расписание сформировалось
        if isinstance(near_lessons, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        # если пар нет
        if not near_lessons:
            await ans.answer('Сегодня больше пар нет 😎', keyboard=keyboards.make_keyboard_start_menu())
            statistics.add(action='Следующая', storage=storage, tz=tz)
            return

        # Студент
        if storage.get_vk_user(chat_id=chat_id)['course'] != 'None':
            near_lessons_str = get_now_lesson_in_str_stud(near_lessons)

        # Преподаватель
        elif storage.get_vk_user(chat_id=chat_id)['course'] == 'None':
            near_lessons_str = get_now_lesson_in_str_prep(near_lessons)

        # Проверяем, что расписание сформировалось
        if isinstance(near_lessons_str, APIError):
            await schedule_processing.sending_schedule_is_not_available(ans=ans)
            return

        await ans.answer(f'🧠Ближайшая пара🧠\n'f'{near_lessons_str}',
                         keyboard=keyboards.make_keyboard_start_menu())

        statistics.add(action='Следующая', storage=storage, tz=tz)
