import json
from typing import Iterable, List

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

def _reply_keyboard(rows: Iterable[List[str]]) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=text) for text in row] for row in rows],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def _inline_keyboard(rows: Iterable[List[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def make_keyboard_start_menu():
    return _reply_keyboard(
        [
            ['Расписание 🗓', 'Ближайшая пара ⏱'],
            ['Расписание на сегодня 🍏'],
            ['Расписание на завтра 🍎'],
            ['Поиск 🔎', 'Другое ⚡'],
        ]
    )


def make_keyboard_search_goal():
    return _reply_keyboard([['Группы и преподаватели'], ['Аудитории'], ['Основное меню']])


def make_keyboard_empty():
    return ReplyKeyboardRemove()


def make_inline_keyboard_choose_institute(institutes=None):
    institutes = institutes or []
    rows = [
        [InlineKeyboardButton(text='Преподаватель', callback_data='inst:teacher')],
    ]
    for index, institute in enumerate(institutes):
        name = institute['name']
        institute_id = institute.get('_id', index)
        rows.append([InlineKeyboardButton(text=name, callback_data=f'inst:{institute_id}')])
    return _inline_keyboard(rows)


def make_inline_keyboard_choose_courses(courses=None):
    courses = courses or []
    rows = [
        [InlineKeyboardButton(text=course['name'], callback_data=f"course:{course.get('_id', index)}")]
        for index, course in enumerate(courses)
    ]
    rows.append([InlineKeyboardButton(text='<', callback_data='course:back')])
    return _inline_keyboard(rows)


def make_inline_keyboard_choose_groups(groups=None):
    groups = groups or []
    rows = [
        [InlineKeyboardButton(text=group['name'], callback_data=f"group:{group.get('_id', index)}")]
        for index, group in enumerate(groups)
    ]
    rows.append([InlineKeyboardButton(text='<', callback_data='group:back')])
    return _inline_keyboard(rows)


def make_inline_keyboard_reg_prep(preps=None):
    preps = preps or []
    rows = [
        [InlineKeyboardButton(text=prep['prep'], callback_data=json.dumps({"prep_id": prep['pg_id']}))]
        for prep in preps
    ]
    rows.append(
        [InlineKeyboardButton(text='Назад к институтам', callback_data=json.dumps({"prep_id": "back"}))]
    )
    return _inline_keyboard(rows)


def make_inline_keyboard_notifications(time=0):
    return _inline_keyboard(
        [
            [InlineKeyboardButton(text='Настройки ⚙', callback_data=json.dumps({"notification_btn": time}))],
            [InlineKeyboardButton(text='Свернуть', callback_data=json.dumps({"notification_btn": "close"}))],
        ]
    )


def make_keyboard_main_menu(mode='prep'):
    callback_prefix = 'aud' if mode == 'aud' else 'prep'
    return _inline_keyboard(
        [[InlineKeyboardButton(text='Основное меню', callback_data=f'{callback_prefix}:main')]]
    )


def make_keyboard_search_group(last_request, page, more_than_10=False, requests=None, start_index=0):
    requests = requests or []
    rows = []
    for index, request in enumerate(requests, start=start_index):
        name = request['found_prep']
        rows.append([InlineKeyboardButton(text=name, callback_data=f'prep:{index}')])

    if page == 0:
        if more_than_10:
            rows.append([InlineKeyboardButton(text='>', callback_data='prep:next')])
        rows.append(
            [InlineKeyboardButton(text='Основное меню', callback_data='prep:main')]
        )
    elif requests and last_request == requests[-1]:
        rows.append([InlineKeyboardButton(text='<', callback_data='prep:back')])
    else:
        rows.append(
            [
                InlineKeyboardButton(text='<', callback_data='prep:back'),
                InlineKeyboardButton(text='>', callback_data='prep:next'),
            ]
        )
    return _inline_keyboard(rows)


def make_keyboard_search_group_aud(last_request, page, more_than_10=False, requests=None, start_index=0):
    requests = requests or []
    rows = []
    for index, request in enumerate(requests, start=start_index):
        name = request['search_aud']
        rows.append([InlineKeyboardButton(text=name, callback_data=f'aud:{index}')])

    if page == 0:
        if more_than_10:
            rows.append([InlineKeyboardButton(text='>', callback_data='aud:next')])
        rows.append(
            [InlineKeyboardButton(text='Основное меню', callback_data='aud:main')]
        )
    elif requests and last_request == requests[-1]:
        rows.append([InlineKeyboardButton(text='<', callback_data='aud:back')])
    else:
        rows.append(
            [
                InlineKeyboardButton(text='<', callback_data='aud:back'),
                InlineKeyboardButton(text='>', callback_data='aud:next'),
            ]
        )
    return _inline_keyboard(rows)


def make_inline_keyboard_aud_results(items):
    rows = []
    row = []
    for index, item in enumerate(items):
        row.append(
            InlineKeyboardButton(
                text=item,
                callback_data=f'aud:{index}',
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text='Основное меню', callback_data='aud:main')])
    return _inline_keyboard(rows)


def make_inline_keyboard_set_notifications(time=0):
    text_check = f'{time} мин' if time != 0 else 'off'
    return _inline_keyboard(
        [
            [
                InlineKeyboardButton(text='-', callback_data=json.dumps({"del_notifications": time})),
                InlineKeyboardButton(text=text_check, callback_data='None'),
                InlineKeyboardButton(text='+', callback_data=json.dumps({"add_notifications": time})),
            ],
            [InlineKeyboardButton(text='Сохранить', callback_data=json.dumps({"save_notifications": time}))],
        ]
    )


def make_inline_keyboard_choose_week():
    return _inline_keyboard(
        [
            [
                InlineKeyboardButton(text='Четная', callback_data='odd'),
                InlineKeyboardButton(text='Нечетная', callback_data='even'),
            ],
            [InlineKeyboardButton(text='Текущая', callback_data='week_now')],
        ]
    )


def make_keyboard_choose_schedule():
    return _reply_keyboard(
        [
            ['На текущую неделю', 'На следующую неделю'],
            ['Экзамены'],
            ['Основное меню'],
        ]
    )


def make_keyboard_choose_schedule_for_aud_search():
    return _reply_keyboard([['На текущую неделю', 'На следующую неделю'], ['Основное меню']])


def make_keyboard_extra():
    return _reply_keyboard([['Помощь'], ['Напоминание 📣'], ['Основное меню']])


def make_keyboard_commands():
    return _reply_keyboard([['Авторы'], ['Регистрация', 'Карта'], ['Основное меню']])


def make_keyboard_nearlesson():
    return _reply_keyboard([['Текущая', 'Следующая'], ['Основное меню']])
