import os
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault('ISTU_API_KEY', 'test-key')

from functions.istu_api_client import ISTUApiClient, candidate_weeks, week_parity_of


def _item(**overrides):
    item = {
        'id': 1, 'type': 'day', 'day': '1', 'para': 2, 'everyweek': 2, 'nt': 1, 'ngroup': None,
        'title': 'Математика', 'groups': [10], 'teachers_ids': [100], 'auditories_ids': [200],
        'auditories_verbose': 'К-303',
    }
    item.update(overrides)
    return item


class TestWeekParity(unittest.TestCase):
    def test_september_first_week_is_even_and_alternates(self):
        # Проверено по реальным страницам ИРНИТУ (см. functions_api/functions/find_week.py).
        self.assertEqual(week_parity_of(date(2026, 8, 31)), 'even')
        self.assertEqual(week_parity_of(date(2026, 9, 7)), 'odd')
        self.assertEqual(week_parity_of(date(2026, 9, 28)), 'even')
        self.assertEqual(week_parity_of(date(2026, 10, 5)), 'odd')

    def test_candidate_weeks_prefers_nearest_week_of_each_parity(self):
        weeks = candidate_weeks(date(2026, 10, 3))  # неделя 28.09 — чётная
        self.assertEqual(weeks['even'][0], date(2026, 9, 28))
        self.assertEqual(weeks['odd'][0], date(2026, 10, 5))
        self.assertTrue(all(week_parity_of(m) == 'odd' for m in weeks['odd']))


class TestApiClient(unittest.TestCase):
    def setUp(self):
        self.client = ISTUApiClient()
        self.titles = {10: 'АД-25-1'}
        self.teachers = {100: 'Иванов И.И.'}
        self.auds = {200: 'К-303'}

    def _parse(self, payloads_by_monday):
        group = {'group_id': 10, 'name': 'АД-25-1'}

        def fake_week(group_id, monday):
            return payloads_by_monday.get(monday, {'schedule': [], 'queries': []})

        with mock.patch.object(self.client, '_fetch_group_week', side_effect=fake_week):
            return self.client._parse_group(group, date(2026, 10, 3), self.titles, self.teachers, self.auds)

    def test_every_week_lesson_collapses_and_parity_lessons_keep_their_week(self):
        every = _item(id=1)
        even_only = _item(id=2, everyweek=1, para=3, title='Физика')
        doc, events = self._parse({
            date(2026, 9, 28): {'schedule': [every, even_only], 'queries': []},
            date(2026, 10, 5): {'schedule': [every], 'queries': []},
        })
        lessons = doc['schedule'][0]['lessons']
        self.assertEqual(
            [(l['time'], l['week'], l['name']) for l in lessons],
            [('10:00', 'all', 'Математика'), ('11:45', 'even', 'Физика')],
        )
        self.assertEqual(lessons[0]['aud'], ['К-303'])
        self.assertEqual(lessons[0]['prep'], ['Иванов И.И.'])
        self.assertEqual(lessons[0]['info'], '( Лекция )')
        self.assertEqual(len(events), 2)

    def test_days_above_seven_are_second_week_days_and_map_to_weekday_by_modulo(self):
        odd_monday = _item(everyweek=1, day=8, para=5, title='Физика', nt=3, ngroup=1)
        doc, _ = self._parse({date(2026, 10, 5): {'schedule': [odd_monday], 'queries': []}})
        self.assertEqual(doc['schedule'][0]['day'], 'понедельник')
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual((lesson['week'], lesson['time']), ('odd', '15:30'))

    def test_subgroup_lab_info_and_missing_aud_and_teacher(self):
        lab = _item(nt=3, ngroup=2, auditories_ids=None, auditories_verbose='', teachers_ids=None)
        doc, _ = self._parse({date(2026, 9, 28): {'schedule': [lab], 'queries': []}})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['info'], '( Лаб. раб. подгруппа 2 )')
        self.assertEqual(lesson['aud'], [''])
        self.assertEqual(lesson['prep'], [''])

    def test_one_time_move_uses_legacy_name_format_with_target_date(self):
        move = _item(id=55, type='query', everyweek=2, day=2, para=5, title='«Геология», перенос из Е-223')
        payload = {'schedule': [move], 'queries': [{'id': 55, 'dt': '2026-10-06'}]}
        doc, _ = self._parse({date(2026, 10, 5): payload})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['name'], 'Разовый перенос «Геология», перенос из Е-223, на 2026.10.06 перенос')
        self.assertEqual(lesson['week'], 'odd')
        self.assertEqual(lesson['time'], '15:30')

    def test_empty_week_falls_back_to_next_nearest_week_of_same_parity(self):
        doc, _ = self._parse({date(2026, 9, 14): {'schedule': [_item(everyweek=1)], 'queries': []}})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['week'], 'even')

    def test_missing_api_key_is_an_error(self):
        with mock.patch.dict(os.environ, {'ISTU_API_KEY': ''}):
            with self.assertRaises(RuntimeError):
                ISTUApiClient()


if __name__ == '__main__':
    unittest.main()
