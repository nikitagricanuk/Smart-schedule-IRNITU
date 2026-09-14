from datetime import datetime, date

import pendulum
import pytz

from functions import schedule_tools

TIME_ZONE = pytz.timezone('Asia/Irkutsk')

DAYS = schedule_tools.DAYS


def get_week_even(dt):
    """
    Возвращает 0 если неделя нечетная, и 1 если неделя четная
    """
    september_1st = datetime(dt.year, 9, 1)

    if dt.month >= 9 or dt.isocalendar()[1] == september_1st.isocalendar()[1]:
        september_1st = datetime(dt.year, 9, 1)
    else:
        september_1st = datetime(dt.year - 1, 9, 1)

    if isinstance(dt, date):
        dt = datetime.combine(dt, datetime.min.time())
    dt = pendulum.instance(dt)
    study_year_start = pendulum.instance(september_1st).start_of("week")
    weeks = (dt - study_year_start).days // 7

    return weeks % 2


def convert_institutes(pg_institutes: list) -> list:
    """Преобразование формата институтов"""
    if not pg_institutes:
        raise ValueError('Данные не могут быть пустыми')
    result_data = [{'name': institute['fac']} for institute in pg_institutes]
    return result_data


def convert_groups(pg_groups: list) -> list:
    """Преобразование формата групп"""
    if not pg_groups:
        raise ValueError('Данные не могут быть пустыми')
    result_data = [{
        'name': group['obozn'],
        'course': f"{group['kurs']} курс",
        'institute': group['fac']
    } for group in pg_groups]

    return result_data


def convert_courses(mongo_groups: list) -> list:
    """Получение курсов из информации о группах"""
    if not mongo_groups:
        raise ValueError('Данные не могут быть пустыми')

    # Удаляем информацию о курсе, меняем ключ course на name
    for group in mongo_groups:
        group.pop('name', None)
        group.pop('_id', None)
        group['name'] = group.pop('course')

    # Удаляем поторяющиеся элементы
    courses = []
    for item in mongo_groups:
        if item not in courses:
            courses.append(item)
    return courses


def convert_teachers(pg_teachers: list) -> list:
    """Преобразование формата преподавателей"""

    if not pg_teachers:
        raise ValueError('Данные не могут быть пустыми')

    mongo_teachers = pg_teachers
    for teacher in mongo_teachers:
        teacher['prep'] = teacher['prep'].strip()
        teacher['prep_short_name'] = teacher['prep_short_name'].strip()

        # Удаляем пустого преподавателя
        if not teacher['prep'] and not teacher['prep_short_name']:
            mongo_teachers.remove(teacher)

    return mongo_teachers


def convert_schedule(pg_schedule: list) -> list:
    """Преобразование формата расписания"""

    date_now = datetime.now(TIME_ZONE).date()

    # Сортируем массив, чтобы одинаковые группы стояли рядом.
    pg_schedule = sorted(pg_schedule, key=lambda x: x['obozn'])
    all_schedule = []

    schedule = []  # Расписание группы.

    item_index = 0  # Счетчик индекса.
    for item in pg_schedule:

        # Проверяем, что расписание действует
        if item['dbeg'] <= date_now <= item['dend']:

            week, day = schedule_tools.getting_week_and_day_of_week(item)

            # Определяем вид пары и подгруппу.
            info = schedule_tools.forming_info_data(nt=item['nt'], ngroup=item["ngroup"])

            lesson = {
                'time': item['begtime'],
                'week': week,
                'name': item['title'],
                'aud': [item['auditories_verbose'] if item['auditories_verbose'] else ''],
                'info': info,
                'prep': [item['preps'].strip().strip('.') if item['preps'] else ''],
            }

            # Смотрим, создал ли уже нужный день в расписании.
            if not schedule_tools.is_there_dict_with_value_in_list(schedule, day):
                schedule.append(
                    {
                        'day': day,
                        'lessons': []
                    }
                )

            # Добавляем пары в нужный день.
            for sch in schedule:
                if sch['day'] == day:
                    if lesson in sch['lessons']:
                        break

                    # Проверяем есть ли уже занятие в расписании
                    for day_lesson in sch['lessons']:
                        # Если есть, добавляем только преподавателя
                        if lesson['time'] == day_lesson['time'] \
                                and lesson['week'] == day_lesson['week'] \
                                and lesson['name'] == day_lesson['name'] \
                                and lesson['info'] == day_lesson['info']:
                            if lesson['aud'] == day_lesson['aud']:
                                if item['preps']:
                                    day_lesson['prep'].append(item['preps'].strip().strip('.'))
                            elif lesson['prep'] == day_lesson['prep']:
                                day_lesson['aud'].append(item['auditories_verbose'])
                            break
                    else:  # Если нет, добавляем полностью пару.
                        sch['lessons'].append(lesson)
                    break

        # Если нашлась другая группа или это последний элемент списка, сохраняем предыдущую.
        current_group = item['obozn']
        next_group = ''
        if item_index != len(pg_schedule) - 1:
            next_group = pg_schedule[item_index + 1]['obozn']

        if current_group != next_group or item_index == len(pg_schedule) - 1:
            # Проверяем, что расписание не пустое
            if schedule:
                # Сортируем пары в дне по времени и подгруппе
                schedule_tools.sorting_lessons_in_a_day_by_time_and_ngroup(schedule=schedule)

                all_schedule.append({
                    'group': current_group,
                    'schedule': schedule_tools.days_in_right_order(schedule)
                })

                # Обнуляем расписание для слудующей группы
                schedule = []

        item_index += 1  # Увеличиваем счетчик индекса.
    return all_schedule


def convert_teachers_schedule(pg_schedule: list) -> list:
    """Преобразование формата расписания преподавателей"""

    date_now = datetime.now(TIME_ZONE).date()

    # Убираем из расписания преподавателей, которые None
    pg_schedule = [item for item in pg_schedule if item['prep_id']]

    # Сортируем массив, чтобы одинаковые преподаватели стояли рядом.
    pg_schedule = sorted(pg_schedule, key=lambda x: x['prep_id'])

    all_schedule = []

    schedule = []  # Расписание преподавателя.

    item_index = 0  # Счетчик индекса.
    for item in pg_schedule:

        # Проверяем, что расписание действует
        if item['dbeg'] <= date_now <= item['dend']:

            week, day = schedule_tools.getting_week_and_day_of_week(item)

            # Определяем вид пары и подгруппу.
            info = schedule_tools.forming_info_data(nt=item['nt'], ngroup=item["ngroup"])

            lesson = {
                'time': item['begtime'],
                'week': week,
                'name': item['title'],
                'aud': [item['auditories_verbose'] if item['auditories_verbose'] else ''],
                'info': info,
                'groups': [item['obozn']],
            }

            # Смотрим, создал ли уже нужный день в расписании.
            if not schedule_tools.is_there_dict_with_value_in_list(schedule, day):
                schedule.append(
                    {
                        'day': day,
                        'lessons': []
                    }
                )

            # Добавляем пары в нужный день.
            for sch in schedule:
                if sch['day'] == day:
                    if lesson in sch['lessons']:
                        break

                    # Проверяем есть ли уже занятие в расписании
                    for day_lesson in sch['lessons']:
                        # Если есть, добавляем только группу
                        if lesson['time'] == day_lesson['time'] \
                                and lesson['week'] == day_lesson['week'] \
                                and lesson['name'] == day_lesson['name'] \
                                and lesson['info'] == day_lesson['info']:
                            if lesson['aud'] == day_lesson['aud']:
                                day_lesson['groups'].append(item['obozn'])
                            elif lesson['groups'] == day_lesson['groups']:
                                day_lesson['aud'].append(item['auditories_verbose'])
                            break
                    else:  # Если нет, добавляем полностью пару.
                        sch['lessons'].append(lesson)
                    break

        # Если нашелся другой преподаватель или это последний элемент списка, сохраняем.
        current_prep_id = item['prep_id']
        next_prep_id = ''
        if item_index != len(pg_schedule) - 1:
            next_prep_id = pg_schedule[item_index + 1]['prep_id']

        if current_prep_id != next_prep_id or item_index == len(pg_schedule) - 1:
            # Проверяем, что расписание не пустое
            if schedule:
                schedule_tools.sorting_lessons_in_a_day_by_time_and_ngroup(schedule=schedule)

                all_schedule.append({
                    'prep': item['preps'].strip(),
                    'prep_short_name': item['prep_short_name'].strip(),
                    'pg_id': current_prep_id,
                    'schedule': schedule_tools.days_in_right_order(schedule)
                })

                # Обнуляем расписание для слудующего преподавателя
                schedule = []

        item_index += 1  # Увеличиваем счетчик индекса.
    return all_schedule


def convert_auditories_schedule(pg_schedule: list) -> list:
    """Преобразование формата расписания аудиторий"""

    date_now = datetime.now(TIME_ZONE).date()

    # Убираем из расписания аудитории, которые None и Онлайн
    pg_schedule = [item for item in pg_schedule if
                   item['auditories_verbose'] and item['auditories_verbose'] != 'онлайн']

    # Сортируем массив, чтобы одинаковые аудитории стояли рядом.
    pg_schedule = sorted(pg_schedule, key=lambda x: x['auditories_verbose'])

    all_schedule = []

    schedule = []  # Расписание аудитории.

    item_index = 0  # Счетчик индекса.
    for item in pg_schedule:

        # Проверяем, что расписание действует и указано название аудитории.
        if item['dbeg'] <= date_now <= item['dend'] and item['auditories_verbose']:

            week, day = schedule_tools.getting_week_and_day_of_week(item)

            # Определяем вид пары и подгруппу.
            info = schedule_tools.forming_info_data(nt=item['nt'], ngroup=item["ngroup"])

            lesson = {
                'time': item['begtime'],
                'week': week,
                'name': item['title'],
                'info': info,
                'prep': [item['preps'].strip().strip('.') if item['preps'] else ''],
                'groups': [item['obozn']],
            }

            # Смотрим, создал ли уже нужный день в расписании.
            if not schedule_tools.is_there_dict_with_value_in_list(schedule, day):
                schedule.append(
                    {
                        'day': day,
                        'lessons': []
                    }
                )

            # Добавляем пары в нужный день.
            for sch in schedule:
                if sch['day'] == day:
                    if lesson in sch['lessons']:
                        break

                    # Проверяем есть ли уже занятие в расписании
                    for day_lesson in sch['lessons']:
                        # Если есть, добавляем только группу или только преподавателя.
                        if lesson['time'] == day_lesson['time'] \
                                and lesson['week'] == day_lesson['week'] \
                                and lesson['name'] == day_lesson['name'] \
                                and lesson['info'] == day_lesson['info']:

                            if lesson['prep'] == day_lesson['prep']:
                                day_lesson['groups'].append(item['obozn'])
                            elif lesson['groups'] == day_lesson['groups']:
                                if item['preps']:
                                    day_lesson['prep'].append(item['preps'].strip().strip('.'))
                            break
                    else:  # Если нет, добавляем полностью пару.
                        sch['lessons'].append(lesson)
                    break

        # Если нашелся другой преподаватель или это последний элемент списка, сохраняем.
        current_aud = item['auditories_verbose']
        next_aud = ''
        if item_index != len(pg_schedule) - 1:
            next_aud = pg_schedule[item_index + 1]['auditories_verbose']

        if current_aud != next_aud or item_index == len(pg_schedule) - 1:
            # Проверяем, что расписание не пустое
            if schedule:
                schedule_tools.sorting_lessons_in_a_day_by_time_and_ngroup(schedule=schedule)

                all_schedule.append({
                    'aud': item['auditories_verbose'],
                    'schedule': schedule_tools.days_in_right_order(schedule)
                })

                # Обнуляем расписание для слудующей аудитории
                schedule = []

        item_index += 1  # Увеличиваем счетчик индекса.
    return all_schedule
