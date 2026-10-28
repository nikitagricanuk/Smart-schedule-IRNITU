import unittest
from datetime import date

from functions.schedule_changes import detect_changed_groups, detect_new_transfers

LAB = {
    'time': '10:00', 'week': 'all', 'name': 'Основы информационной безопасности',
    'info': '( Лаб. раб. подгруппа 2 )', 'aud': ['Ж-312'], 'prep': ['Гордин А.С.'],
}
TRANSFER = {'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00', 'to_aud': ['В-104'],
            'reason': 'инженерные каникулы'}
PROJECT = {'time': '8:15', 'week': 'even', 'name': 'Ярмарка проектов', 'info': '( Лекция )', 'aud': [''],
           'prep': [''], 'date': '2026-10-26'}


def _doc(*lessons):
    return {'group': 'ИСТб-24-2', 'schedule': [{'day': 'среда', 'lessons': list(lessons)}]}


class TestScheduleChanges(unittest.TestCase):
    def test_new_transfer_is_not_a_regular_schedule_change_but_is_reported(self):
        old = [_doc(LAB, PROJECT)]
        new = [_doc(dict(LAB, transfers=[TRANSFER]), PROJECT)]
        self.assertEqual(detect_changed_groups(old, new), [])

        found = detect_new_transfers(old, new, today=date(2026, 10, 27))
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]['group'], 'ИСТб-24-2')
        transfer = found[0]['transfer']
        self.assertEqual(transfer['subgroup'], 2)
        self.assertEqual(transfer['aud'], ['Ж-312'])
        self.assertEqual(transfer['to_aud'], ['В-104'])
        self.assertEqual(transfer['name'], 'Основы информационной безопасности')

    def test_known_and_past_transfers_are_not_reported_again(self):
        old = [_doc(dict(LAB, transfers=[TRANSFER]))]
        self.assertEqual(detect_new_transfers(old, old, today=date(2026, 10, 27)), [])
        new = [_doc(dict(LAB, transfers=[TRANSFER, dict(TRANSFER, date='2026-10-21', to_date='2026-10-21')]))]
        self.assertEqual(detect_new_transfers(old, new, today=date(2026, 10, 27)), [])

    def test_first_run_after_upgrade_does_not_notify_about_every_existing_transfer(self):
        legacy = [_doc(LAB, dict(LAB, name='Разовый перенос «X», перенос из Ж-312, на 2026.10.28 перенос'))]
        new = [_doc(dict(LAB, transfers=[TRANSFER]))]
        self.assertEqual(detect_new_transfers(legacy, new, today=date(2026, 10, 27)), [])
        self.assertEqual(detect_changed_groups(legacy, new), [])

    def test_unmatched_transfer_is_reported(self):
        old = [_doc(LAB, PROJECT)]
        one_off = {'time': '11:45', 'week': 'even', 'name': '«Физика», перенос из В-208', 'info': '( Лекция )',
                   'aud': ['Б-118а'], 'prep': [''], 'date': '2026-10-29', 'transfer': True}
        found = detect_new_transfers(old, [_doc(LAB, PROJECT, one_off)], today=date(2026, 10, 27))
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]['transfer']['to_date'], '2026-10-29')

    def test_regular_change_is_still_detected(self):
        old = [_doc(LAB)]
        new = [_doc(dict(LAB, aud=['Ж-313']))]
        self.assertEqual(detect_changed_groups(old, new), ['ИСТб-24-2'])


if __name__ == '__main__':
    unittest.main()
