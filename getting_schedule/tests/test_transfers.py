import unittest
from datetime import date

from functions.transfers import find_original_lesson, is_transfer, parse_transfer_title, transfer_target_auditories


def _lesson(name, time='10:00', week='all', info='( Лекция )', aud=None):
    return {'name': name, 'time': time, 'week': week, 'info': info, 'aud': aud or ['А-100'], 'prep': ['']}


class TestParseTransferTitle(unittest.TestCase):
    """Варианты текста — из реальных карточек API ИРНИТУ (октябрь 2026)."""

    def test_room_transfer_with_subgroup_and_reason(self):
        parsed = parse_transfer_title(
            '2-ая подгруппа «Основы информационной безопасности», перенос из Ж-312, причина: инженерные каникулы')
        self.assertEqual(parsed['subject'], 'Основы информационной безопасности')
        self.assertEqual(parsed['subgroup'], 2)
        self.assertEqual(parsed['from_aud'], 'Ж-312')
        self.assertIsNone(parsed['from_date'])
        self.assertEqual(parsed['reason'], 'инженерные каникулы')

    def test_room_transfer_without_reason_and_lecture_hall(self):
        parsed = parse_transfer_title('«Детали машин», перенос из Г-амф.')
        self.assertEqual(parsed['subject'], 'Детали машин')
        self.assertIsNone(parsed['subgroup'])
        self.assertEqual(parsed['from_aud'], 'Г-амф')
        self.assertIsNone(parsed['reason'])

    def test_date_transfer(self):
        parsed = parse_transfer_title('1-ая подгруппа «Базы данных», перенос с 2026.10.23')
        self.assertEqual(parsed['subgroup'], 1)
        self.assertEqual(parsed['from_date'], date(2026, 10, 23))
        self.assertIsNone(parsed['from_aud'])

    def test_slot_transfer(self):
        parsed = parse_transfer_title(
            '2-ая подгруппа «Проектирование в графическом дизайне и рекламе», перенос с 2-ой пары, причина: занятия')
        self.assertEqual(parsed['from_para'], 2)
        self.assertIsNone(parsed['from_date'])
        self.assertEqual(parsed['reason'], 'занятия')

    def test_reason_only(self):
        parsed = parse_transfer_title('«Облачные вычисления / Cloud Computing », причина: День открытых дверей')
        self.assertEqual(parsed['subject'], 'Облачные вычисления / Cloud Computing')
        self.assertEqual(parsed['reason'], 'День открытых дверей')


class TestTransferQueries(unittest.TestCase):
    def test_only_type_2_queries_are_transfers(self):
        self.assertTrue(is_transfer({'type': 2}, 'что угодно'))
        self.assertFalse(is_transfer({'type': 0}, 'Ярмарка проектов'))
        self.assertTrue(is_transfer(None, '«Геология», перенос из Е-223'))
        self.assertFalse(is_transfer(None, 'Интерактивная игра'))

    def test_new_room_comes_from_the_transfer_order(self):
        query = {'type': 2, 'related_queries': [
            {'type': 1, 'auds': [505], 'title': 'День открытых дверей | перевод ИСТб-24-2-1, Бучнев О.С. в Б-118а'},
        ]}
        self.assertEqual(transfer_target_auditories(query, {505: 'Б-118а'}), ['Б-118а'])
        self.assertEqual(transfer_target_auditories(query, {}), ['Б-118а'])
        self.assertEqual(transfer_target_auditories({'type': 2}, {}), [])


class TestFindOriginalLesson(unittest.TestCase):
    def test_picks_the_subgroup_from_the_transfer_text(self):
        first = _lesson('Моделирование процессов и систем', time='13:45', info='( Лаб. раб. подгруппа 1 )')
        second = _lesson('Моделирование процессов и систем', time='13:45', info='( Лаб. раб. подгруппа 2 )')
        parsed = parse_transfer_title(
            '1-ая подгруппа «Моделирование процессов и систем», перенос из В-208, причина: День открытых дверей')
        found = find_original_lesson({'суббота': [first, second]}, parsed, date(2026, 10, 31), '13:45')
        self.assertIs(found, first)

    def test_respects_week_parity_of_the_original_date(self):
        odd = _lesson('Физика', week='odd')
        parsed = parse_transfer_title('«Физика», перенос из В-208')
        # 28.10.2026 — чётная неделя, нечётная пара не подходит.
        self.assertIsNone(find_original_lesson({'среда': [odd]}, parsed, date(2026, 10, 28), '10:00'))

    def test_ambiguous_match_is_not_applied(self):
        lessons = [_lesson('Физика', time='10:00'), _lesson('Физика', time='11:45')]
        parsed = parse_transfer_title('«Физика», причина: занятия')
        self.assertIsNone(find_original_lesson({'среда': lessons}, parsed, date(2026, 10, 28), '15:30'))
        # но если время совпадает с одной из пар — это она
        self.assertIs(find_original_lesson({'среда': lessons}, parsed, date(2026, 10, 28), '11:45'), lessons[1])

    def test_slot_transfer_uses_the_original_para(self):
        lessons = [_lesson('Физика', time='10:00'), _lesson('Физика', time='15:30')]
        parsed = parse_transfer_title('«Физика», перенос с 5-ой пары')
        found = find_original_lesson({'среда': lessons}, parsed, date(2026, 10, 28), '8:15', '15:30')
        self.assertIs(found, lessons[1])


if __name__ == '__main__':
    unittest.main()
