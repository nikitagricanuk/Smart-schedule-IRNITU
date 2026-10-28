import unittest
from datetime import date

from functions.creating_schedule import full_schedule_in_str
from functions.dated_lessons import lessons_on_date

LAB = {
    'time': '10:00', 'week': 'all', 'name': 'Основы информационной безопасности',
    'info': '( Лаб. раб. подгруппа 2 )', 'aud': ['Ж-312'], 'prep': ['Гордин А.С.'],
    'transfers': [{'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00', 'to_aud': ['В-104'],
                   'reason': 'инженерные каникулы'}],
}
MOVED = {
    'time': '8:15', 'week': 'even', 'name': 'Базы данных', 'info': '( Лаб. раб. подгруппа 1 )',
    'aud': ['В-202'], 'prep': [''],
    'transfers': [{'date': '2026-10-26', 'to_date': '2026-10-29', 'to_time': '11:45', 'to_aud': ['Б-118а']}],
}
PROJECT = {'time': '17:10', 'week': 'even', 'name': 'Ярмарка проектов', 'info': '( Лекция )', 'aud': [''],
           'prep': [''], 'date': '2026-10-28'}
LEGACY = {'time': '17:10', 'week': 'even', 'name': 'Разовый перенос Физика, на 2026.11.11 перенос',
          'info': '( Лекция )', 'aud': [''], 'prep': ['']}
SCHEDULE = [
    {'day': 'понедельник', 'lessons': [MOVED]},
    {'day': 'среда', 'lessons': [LAB, PROJECT, LEGACY]},
]


class TestLessonsOnDate(unittest.TestCase):
    def test_room_transfer_replaces_the_room_on_that_date_only(self):
        lessons = lessons_on_date(SCHEDULE, date(2026, 10, 28), 'even')
        self.assertEqual([l['name'] for l in lessons], ['Основы информационной безопасности', 'Ярмарка проектов'])
        self.assertEqual(lessons[0]['aud'], ['В-104'])
        self.assertIn('moved_in', lessons[0])

        next_week = lessons_on_date(SCHEDULE, date(2026, 11, 4), 'odd')
        self.assertEqual([l['aud'] for l in next_week], [['Ж-312']])

    def test_legacy_one_off_shows_only_on_its_date(self):
        self.assertEqual(len(lessons_on_date(SCHEDULE, date(2026, 10, 28), 'even')), 2)
        names = [l['name'] for l in lessons_on_date(SCHEDULE, date(2026, 11, 11), 'even')]
        self.assertIn('Разовый перенос Физика, на 2026.11.11 перенос', names)

    def test_date_transfer_cancels_original_day_and_adds_new_one(self):
        monday = lessons_on_date(SCHEDULE, date(2026, 10, 26), 'even')
        self.assertEqual(len(monday), 1)
        self.assertIn('moved_away', monday[0])

        thursday = lessons_on_date(SCHEDULE, date(2026, 10, 29), 'even')
        self.assertEqual(len(thursday), 1)
        self.assertEqual((thursday[0]['time'], thursday[0]['aud']), ('11:45', ['Б-118а']))

    def test_time_only_transfer_moves_the_class_within_the_day(self):
        lab = dict(LAB, transfers=[{'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '15:30',
                                    'to_aud': ['Ж-312']}])
        lessons = lessons_on_date([{'day': 'среда', 'lessons': [lab]}], date(2026, 10, 28), 'even')
        self.assertEqual([(l['time'], 'moved_in' in l) for l in lessons], [('15:30', True)])

    def test_transfer_from_odd_week_class_to_even_week_date(self):
        odd = dict(MOVED, week='odd', transfers=[{'date': '2026-11-02', 'to_date': '2026-10-29', 'to_time': '8:15',
                                                  'to_aud': ['В-202']}])
        schedule = [{'day': 'понедельник', 'lessons': [odd]}]
        self.assertEqual(len(lessons_on_date(schedule, date(2026, 10, 29), 'even')), 1)
        self.assertIn('moved_away', lessons_on_date(schedule, date(2026, 11, 2), 'odd')[0])
        self.assertEqual(lessons_on_date(schedule, date(2026, 10, 26), 'even'), [])

    def test_two_transfers_of_the_same_class(self):
        lab = dict(LAB, transfers=[LAB['transfers'][0], {'date': '2026-11-04', 'to_date': '2026-11-05',
                                                          'to_time': '10:00', 'to_aud': ['Ж-312']}])
        schedule = [{'day': 'среда', 'lessons': [lab]}]
        self.assertEqual(lessons_on_date(schedule, date(2026, 10, 28), 'even')[0]['aud'], ['В-104'])
        self.assertIn('moved_away', lessons_on_date(schedule, date(2026, 11, 4), 'odd')[0])
        self.assertEqual(len(lessons_on_date(schedule, date(2026, 11, 5), 'odd')), 1)
        self.assertNotIn('moved_in', lessons_on_date(schedule, date(2026, 11, 11), 'even')[0])


class TestScheduleText(unittest.TestCase):
    def test_week_text_shows_new_room_and_what_changed(self):
        text = ''.join(full_schedule_in_str(SCHEDULE, 'even', monday='2026-10-26'))
        self.assertIn('Аудитория: В-104\n👉Основы информационной безопасности', text)
        self.assertIn('🔄 Разовый перенос: ауд. Ж-312 → В-104 (инженерные каникулы)', text)
        self.assertIn('🚫 Пары не будет: перенесена на 29.10 в 11:45, ауд. Б-118а', text)
        self.assertIn('🔄 Разовый перенос: с 26.10, время 8:15 → 11:45, ауд. В-202 → Б-118а', text)
        self.assertNotIn('Разовый перенос Физика', text)
        self.assertNotIn('Ж-312\n', text)


if __name__ == '__main__':
    unittest.main()
