import os
import unittest

from icalendar import Calendar

import app as calendar_app
import lesson_filters

HERE = os.path.dirname(os.path.abspath(__file__))


def _lesson(name, info, week='all', time='10:00'):
    return {'name': name, 'info': info, 'week': week, 'time': time, 'aud': ['В-108'], 'prep': [], 'groups': []}


SCHEDULE = [
    {'day': 'понедельник', 'lessons': [
        _lesson('Управление проектами', '( Проект )', week='even'),
        _lesson('Программирование', '( Лекция )', time='08:15'),
        _lesson('Программирование', '( Лаб. раб. подгруппа 2 )', time='11:45'),
    ]},
    {'day': 'вторник', 'lessons': [
        _lesson('Философия', '( Практ. )'),
        _lesson('Разовый перенос «Философия», на 2026.09.19', '( Практ. )', time='13:45'),
        _lesson('свободно', ''),
    ]},
]


def _names(schedule):
    return [lesson['name'] for day in schedule for lesson in day['lessons']]


class TestLessonFilters(unittest.TestCase):
    def test_copies_in_both_services_are_identical(self):
        with open(os.path.join(HERE, '..', 'lesson_filters.py'), encoding='utf-8') as f:
            calendar_copy = f.read()
        with open(os.path.join(HERE, '..', '..', 'tg_bot', 'tools', 'lesson_filters.py'), encoding='utf-8') as f:
            bot_copy = f.read()
        self.assertEqual(calendar_copy, bot_copy)

    def test_lesson_type_strips_parens_and_subgroup(self):
        self.assertEqual(lesson_filters.lesson_type({'info': '( Лаб. раб. подгруппа 2 )'}), 'Лаб. раб.')
        self.assertEqual(lesson_filters.lesson_type({'info': '( Проект )'}), 'Проект')
        self.assertEqual(lesson_filters.lesson_type({'info': ''}), '')

    def test_options_list_types_and_subjects_without_transfers(self):
        self.assertEqual(lesson_filters.type_options(SCHEDULE), ['Лаб. раб.', 'Лекция', 'Практ.', 'Проект'])
        self.assertEqual(lesson_filters.lesson_options(SCHEDULE), [
            'Программирование|Лаб. раб.',
            'Программирование|Лекция',
            'Управление проектами|Проект',
            'Философия|Практ.',
        ])

    def test_no_rules_returns_schedule_unchanged(self):
        self.assertIs(lesson_filters.filter_schedule(SCHEDULE, {'chat_id': 1}), SCHEDULE)

    def test_hidden_type_removes_every_lesson_of_that_type(self):
        result = lesson_filters.filter_schedule(SCHEDULE, {'hidden_lesson_types': ['Проект']})
        self.assertNotIn('Управление проектами', _names(result))
        self.assertEqual(len(_names(result)), len(_names(SCHEDULE)) - 1)

    def test_hidden_subject_keeps_other_types_of_same_subject(self):
        result = lesson_filters.filter_schedule(SCHEDULE, {'hidden_lessons': ['Программирование|Лекция']})
        programming = [lesson['info'] for day in result for lesson in day['lessons']
                       if lesson['name'] == 'Программирование']
        self.assertEqual(programming, ['( Лаб. раб. подгруппа 2 )'])

    def test_hidden_subject_also_hides_its_one_time_transfers(self):
        result = lesson_filters.filter_schedule(SCHEDULE, {'hidden_lessons': ['Философия|Практ.']})
        self.assertFalse([name for name in _names(result) if 'Философия' in name])


class _FakeStorage:
    def __init__(self, user):
        self.user = user

    def get_user_by_calendar_token(self, token):
        return self.user if token == 'secret' else None

    def get_schedule(self, group_name):
        return {'group': group_name, 'schedule': SCHEDULE}

    def get_schedule_prep(self, prep_name):
        return None


class TestPersonalCalendarRoute(unittest.TestCase):
    def setUp(self):
        self._orig_storage = calendar_app.storage
        self.client = calendar_app.app.test_client()

    def tearDown(self):
        calendar_app.storage = self._orig_storage

    def _summaries(self, user, token='secret'):
        calendar_app.storage = _FakeStorage(user)
        response = self.client.get(f'/ical/u/{token}.ics')
        if response.status_code != 200:
            return response.status_code, None
        cal = Calendar.from_ical(response.data)
        return 200, [str(event['summary']) for event in cal.walk('VEVENT')]

    def test_unknown_token_is_404(self):
        status, _ = self._summaries({'group': 'ИСТб-24-2', 'course': '2'}, token='nope')
        self.assertEqual(status, 404)

    def test_personal_feed_applies_hidden_types_and_subgroup(self):
        status, summaries = self._summaries({
            'group': 'ИСТб-24-2', 'course': '2', 'subgroup': 1,
            'hidden_lesson_types': ['Проект'],
        })
        self.assertEqual(status, 200)
        self.assertFalse([s for s in summaries if 'Управление проектами' in s])
        # Лаба второй подгруппы отфильтрована подгруппой пользователя.
        self.assertFalse([s for s in summaries if 'подгруппа 2' in s])
        self.assertTrue([s for s in summaries if 'Философия' in s])


if __name__ == '__main__':
    unittest.main()
