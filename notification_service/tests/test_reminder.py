import unittest
from datetime import datetime

import pytz

from reminder import Reminder

TZ = pytz.timezone('Asia/Irkutsk')


def _at(day, hour, minute):
    return TZ.localize(datetime(2026, 10, day, hour, minute))


# Среда 28.10.2026 и четверг 29.10.2026 — чётная неделя.
LAB = {
    'time': '10:00', 'week': 'all', 'name': 'Основы информационной безопасности',
    'info': '( Лаб. раб. подгруппа 2 )', 'aud': ['Ж-312'], 'prep': ['Гордин А.С.'],
}
ROOM = {'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00', 'to_aud': ['В-104'],
        'reason': 'инженерные каникулы'}
TIME = {'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '15:30', 'to_aud': ['Ж-312']}
DATE = {'date': '2026-10-28', 'to_date': '2026-10-29', 'to_time': '10:00', 'to_aud': ['Ж-312']}
ALL = {'date': '2026-10-28', 'to_date': '2026-10-30', 'to_time': '11:45', 'to_aud': ['Б-118а']}
ODD_ONLY = dict(LAB, week='odd', name='Физика')
ONE_OFF = {'time': '17:10', 'week': 'even', 'name': 'Ярмарка проектов', 'info': '( Лекция )', 'aud': ['В-301'],
           'prep': [''], 'date': '2026-10-29'}


class FakeStorage:
    def __init__(self, schedule):
        self.schedule = schedule
        self.calls = 0

    def get_schedule(self, group):
        self.calls += 1
        return {'group': group, 'schedule': self.schedule}


class TestRemindersFollowTransfers(unittest.TestCase):
    USER = {'chat_id': 1, 'group': 'ИСТб-24-2', 'notifications': 10}

    def _due(self, transfers, now, lessons=None, users=None):
        lab = dict(LAB, transfers=transfers) if transfers else LAB
        schedule = [{'day': 'среда', 'lessons': [lab] + (lessons or [])}]
        storage = FakeStorage(schedule)
        reminder = Reminder(bot_platform='tg', bot=None, storage=storage)
        return reminder.due_reminders(users or [self.USER], now), storage

    def test_regular_class_reminds_ten_minutes_before(self):
        due, _ = self._due(None, _at(28, 9, 50))
        self.assertEqual(len(due), 1)
        self.assertIn('Аудитория: Ж-312', due[0][1])
        self.assertEqual(self._due(None, _at(28, 9, 49))[0], [])

    def test_room_transfer_reminds_with_new_room(self):
        due, _ = self._due([ROOM], _at(28, 9, 50))
        self.assertIn('Аудитория: В-104', due[0][1])
        self.assertIn('ауд. Ж-312 → В-104', due[0][1])

    def test_time_transfer_reminds_at_new_time_only(self):
        self.assertEqual(self._due([TIME], _at(28, 9, 50))[0], [])
        due, _ = self._due([TIME], _at(28, 15, 20))
        self.assertEqual(len(due), 1)
        self.assertIn('время 10:00 → 15:30', due[0][1])

    def test_date_transfer_reminds_on_new_date_only(self):
        self.assertEqual(self._due([DATE], _at(28, 9, 50))[0], [])
        due, _ = self._due([DATE], _at(29, 9, 50))
        self.assertEqual(len(due), 1)
        self.assertIn('с 28.10', due[0][1])
        # следующая среда — снова как обычно
        self.assertEqual(len(self._due([DATE], TZ.localize(datetime(2026, 11, 4, 9, 50)))[0]), 1)

    def test_date_time_and_room_transfer(self):
        self.assertEqual(self._due([ALL], _at(28, 9, 50))[0], [])
        self.assertEqual(self._due([ALL], _at(30, 9, 50))[0], [])
        due, _ = self._due([ALL], _at(30, 11, 35))
        self.assertIn('Аудитория: Б-118а', due[0][1])
        self.assertIn('с 28.10, время 10:00 → 11:45, ауд. Ж-312 → Б-118а', due[0][1])

    def test_week_parity_and_one_offs(self):
        # нечётная пара не напоминается в чётную неделю
        self.assertEqual(self._due(None, _at(28, 9, 50), lessons=[ODD_ONLY])[0][0][1].count('Физика'), 0)
        self.assertEqual(self._due(None, _at(29, 17, 0), lessons=[ONE_OFF])[0][0][1].count('Ярмарка'), 1)
        self.assertEqual(self._due(None, TZ.localize(datetime(2026, 11, 12, 17, 0)), lessons=[ONE_OFF])[0], [])

    def test_reminder_across_midnight_and_disabled_users(self):
        night = [{'day': 'четверг', 'lessons': [dict(LAB, time='0:20')]}]
        reminder = Reminder(bot_platform='tg', bot=None, storage=FakeStorage(night))
        due = reminder.due_reminders([dict(self.USER, notifications=30)], _at(28, 23, 50))
        self.assertEqual(len(due), 1)
        off = [dict(self.USER, notifications=0), dict(self.USER, group=None)]
        self.assertEqual(reminder.due_reminders(off, _at(28, 23, 50)), [])

    def test_schedule_is_loaded_once_per_group(self):
        users = [dict(self.USER, chat_id=i) for i in range(1, 6)]
        due, storage = self._due(None, _at(28, 9, 50), users=users)
        self.assertEqual(len(due), 5)
        self.assertEqual(storage.calls, 1)


class TestReminderMethods(unittest.TestCase):

    def test_initReminder_wrongPlatform_Exception(self):
        input_value = {
            'bot_platform': 'wrong_platform',
            'bot': None,
            'storage': None
        }

        with self.assertRaises(ValueError):
            reminder = Reminder(**input_value)


    def test_initReminder_RightPlatformTg(self):
        input_value = {
            'bot_platform': 'tg',
            'bot': None,
            'storage': None
        }

        reminder = Reminder(**input_value)

        self.assertIsNotNone(reminder)

    def test_initReminder_RightPlatformVk(self):
        input_value = {
            'bot_platform': 'vk',
            'bot': None,
            'storage': None
        }

        reminder = Reminder(**input_value)

        self.assertIsNotNone(reminder)


if __name__ == '__main__':
    unittest.main()
