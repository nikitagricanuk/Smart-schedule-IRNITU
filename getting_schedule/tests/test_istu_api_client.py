import os
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault('ISTU_API_KEY', 'test-key')

from functions.istu_api_client import ISTUApiClient, candidate_weeks, week_parity_of
from functions.schedule_builder import build_teacher_and_auditory_schedules


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

    def _parse(self, payloads_by_monday, extra_transfers=None):
        group = {'group_id': 10, 'name': 'АД-25-1'}

        def fake_week(group_id, monday):
            return payloads_by_monday.get(monday, {'schedule': [], 'queries': []})

        # Разовые переносы приходят отдельным запросом с широким диапазоном.
        transfers = {'schedule': [], 'queries': []}
        for payload in payloads_by_monday.values():
            transfers['queries'] += payload.get('queries', [])
            transfers['schedule'] += [i for i in payload['schedule'] if i.get('type') == 'query']
        transfers.update(extra_transfers or {})

        with mock.patch.object(self.client, '_fetch_group_week', side_effect=fake_week), \
                mock.patch.object(self.client, '_fetch_group_transfers', return_value=transfers):
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

    def test_unmatched_transfer_stays_a_one_off_lesson_on_its_date(self):
        move = _item(id=55, type='query', everyweek=2, day=2, para=5, title='«Геология», перенос из Е-223')
        payload = {'schedule': [move], 'queries': [{'id': 55, 'dt': '2026-10-06'}]}
        doc, _ = self._parse({date(2026, 10, 5): payload})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['name'], '«Геология», перенос из Е-223')
        self.assertEqual(lesson['date'], '2026-10-06')
        self.assertEqual(lesson['week'], 'odd')
        self.assertEqual(lesson['time'], '15:30')

    def test_project_activity_far_in_the_future_is_a_dated_one_off(self):
        project = _item(id=77, type='query', day=3, para=2, title='Модернизация &quot;Ярмарки проектов&quot;')
        doc, _ = self._parse(
            {date(2026, 9, 28): {'schedule': [_item()], 'queries': []}},
            extra_transfers={'schedule': [project], 'queries': [{'id': 77, 'dt': '2026-11-25', 'type': 0}]},
        )
        one_offs = [l for d in doc['schedule'] for l in d['lessons'] if l.get('date')]
        self.assertEqual(len(one_offs), 1)
        self.assertEqual(one_offs[0]['name'], 'Модернизация "Ярмарки проектов"')
        self.assertEqual(one_offs[0]['date'], '2026-11-25')
        self.assertEqual(one_offs[0]['week'], week_parity_of(date(2026, 11, 23)))
        self.assertNotIn('transfers', [k for d in doc['schedule'] for l in d['lessons'] for k in l if not l.get('date')])

    def test_empty_week_falls_back_to_next_nearest_week_of_same_parity(self):
        doc, _ = self._parse({date(2026, 9, 14): {'schedule': [_item(everyweek=1)], 'queries': []}})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['week'], 'even')

    def test_missing_api_key_is_an_error(self):
        with mock.patch.dict(os.environ, {'ISTU_API_KEY': ''}):
            with self.assertRaises(RuntimeError):
                ISTUApiClient()


class TestRoomTransfer(unittest.TestCase):
    """Реальный случай ИСТб-24-2 (данные API за неделю 26.10.2026)."""

    def setUp(self):
        self.client = ISTUApiClient()
        self.titles = {478236: 'ИСТб-24-2'}
        self.teachers = {5160: 'Гордин А.С.'}
        # Реальные id из справочника auditory/ (проверено 28.10.2026).
        self.auds = {474: 'В-104', 825: 'Ж-312', 909: 'Ж-313'}

    def _lab(self, **overrides):
        lab = _item(id=13430001, day='3', para=2, everyweek=1, nt=3, ngroup=2, title='Основы информационной безопасности',
                    groups=[478236], teachers_ids=[5160], auditories_ids=[825], auditories_verbose='Ж-312')
        lab.update(overrides)
        return lab

    def _transfer_payload(self):
        transfer = _item(id=193346, type='query', day='3', para=2, everyweek=2, nt=1, ngroup=None, groups=[],
                         teachers_ids=[5160], auditories_ids=[474], auditories_verbose='',
                         title='2-ая подгруппа «Основы информационной безопасности», перенос из Ж-312, '
                               'причина: инженерные каникулы')
        query = {
            'id': 193346, 'day': 3, 'dt': '2026-10-28', 'para': 2, 'type': 2, 'auds': [474], 'groups': [478236],
            'title': transfer['title'],
            'related_queries': [
                {'id': 193345, 'type': 1, 'dt': '2026-10-28', 'para': 2, 'auds': [825],
                 'title': 'инженерные каникулы | перевод ИСТб-24-2-2, Гордин А.С. в В-104'},
            ],
        }
        return transfer, query

    def _parse(self, weeks):
        transfer, query = self._transfer_payload()
        group = {'group_id': 478236, 'name': 'ИСТб-24-2'}

        def fake_week(group_id, monday):
            return weeks.get(monday, {'schedule': [], 'queries': []})

        with mock.patch.object(self.client, '_fetch_group_week', side_effect=fake_week), \
                mock.patch.object(self.client, '_fetch_group_transfers',
                                  return_value={'schedule': [transfer], 'queries': [query]}):
            return self.client._parse_group(group, date(2026, 10, 28), self.titles, self.teachers, self.auds)

    def test_transfer_moves_the_original_lab_to_the_new_room_instead_of_adding_a_class(self):
        transfer, query = self._transfer_payload()
        # На неделе переноса API убирает исходную пару, оставляя только карточку переноса;
        # регулярная пара берётся со следующей недели той же чётности (09.11).
        doc, events = self._parse({
            date(2026, 10, 26): {'schedule': [transfer], 'queries': [query]},
            date(2026, 11, 9): {'schedule': [self._lab()], 'queries': []},
        })
        lessons = [l for d in doc['schedule'] for l in d['lessons']]
        self.assertEqual(len(lessons), 1)
        lab = lessons[0]
        self.assertEqual(lab['name'], 'Основы информационной безопасности')
        self.assertEqual(lab['info'], '( Лаб. раб. подгруппа 2 )')
        self.assertEqual(lab['aud'], ['Ж-312'])
        self.assertEqual(lab['transfers'], [{
            'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00', 'to_aud': ['В-104'],
            'reason': 'инженерные каникулы',
        }])

        teacher_docs, aud_docs, _ = build_teacher_and_auditory_schedules(events)
        teacher_lessons = [l for d in teacher_docs[0]['schedule'] for l in d['lessons']]
        self.assertEqual(len(teacher_lessons), 1)
        self.assertEqual(teacher_lessons[0]['transfers'][0]['to_aud'], ['В-104'])
        new_room = [doc for doc in aud_docs if doc['aud'] == 'В-104'][0]
        self.assertEqual(new_room['schedule'][0]['lessons'][0]['date'], '2026-10-28')

    def test_unmatched_transfer_keeps_subgroup_from_text_and_new_room(self):
        # Исходной пары нет ни на одной неделе — перенос остаётся разовым занятием.
        doc, _ = self._parse({})
        lesson = doc['schedule'][0]['lessons'][0]
        self.assertEqual(lesson['date'], '2026-10-28')
        self.assertEqual(lesson['info'], '( Лекция подгруппа 2 )')
        self.assertEqual(lesson['aud'], ['В-104'])
        self.assertEqual(doc['schedule'][0]['day'], 'среда')


if __name__ == '__main__':
    unittest.main()
