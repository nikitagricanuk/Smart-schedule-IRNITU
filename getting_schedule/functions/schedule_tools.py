DAYS = {
    1: 'понедельник',
    2: 'вторник',
    3: 'среда',
    4: 'четверг',
    5: 'пятница',
    6: 'суббота',
    7: 'воскресенье'
}


def getting_week_and_day_of_week(pg_lesson: dict) -> tuple:
    """Определение четности недели и дня недели"""

    day = (pg_lesson['day'] - 1) % 7 + 1
    if pg_lesson['everyweek'] == 2:
        week = 'all'
    else:
        if pg_lesson['day'] <= 7:
            week = 'odd'
        else:
            week = 'even'

    return week, DAYS[day]


def is_there_dict_with_value_in_list(input_list_with_dict: list, value: str) -> bool:
    if not input_list_with_dict:
        return False

    for dict_item in input_list_with_dict:
        if value in dict_item.values():
            return True
    return False


def get_dict_key(d, value):
    """Получение ключа по значеню словаря"""
    for k, v in d.items():
        if v == value:
            return k


def forming_info_data(nt: int, ngroup: str) -> str:
    """Определение вида пары и подгруппы."""
    if nt == 1:
        info = '( Лекция )'
    elif nt == 2:
        if ngroup:
            info = f'( Практ. подгруппа {ngroup} )'
        else:
            info = '( Практ. )'
    else:
        if ngroup:
            info = f'( Лаб. раб. подгруппа {ngroup} )'
        else:
            info = f'( Лаб. раб. )'
    return info


def sorting_lessons_in_a_day_by_time_and_ngroup(schedule: list):
    """Сортировка пар в дне по времени и подгруппе"""
    for sch in schedule:
        # Сортируем подгруппы
        sch['lessons'] = sorted(sch['lessons'], key=lambda x: x['info'])
        # Сортируем по времени
        sch['lessons'] = sorted(sch['lessons'], key=lambda x: int(x['time'].replace(':', '')))

    return schedule


def days_in_right_order(schedule: list) -> list:
    schedule = sorted(schedule, key=lambda x: get_dict_key(DAYS, x['day']))
    return schedule
