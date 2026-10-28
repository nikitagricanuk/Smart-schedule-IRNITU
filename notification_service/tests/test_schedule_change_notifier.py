import unittest
from datetime import datetime

from aiogram.utils.exceptions import BotBlocked, NetworkError

from schedule_change_notifier import ScheduleChangeNotifier, transfer_text

NOW = datetime(2026, 10, 28, 1, 10, 0)


class FakeBot:
    def __init__(self, errors=None):
        self.sent = []
        self.errors = errors or {}

    def send_message(self, chat_id, text):
        if chat_id in self.errors:
            raise self.errors[chat_id]
        self.sent.append((chat_id, text))


class FakeStorage:
    def __init__(self, changes, users_by_group):
        self._changes = changes
        self._users_by_group = users_by_group
        self.marked_notified = []
        self.gave_up = []

    def get_pending_schedule_changes(self):
        return [c for c in self._changes if c['_id'] not in self.marked_notified]

    def get_tg_users_for_group(self, group):
        return self._users_by_group.get(group, [])

    def mark_schedule_change_notified(self, change_id, gave_up=False):
        self.marked_notified.append(change_id)
        if gave_up:
            self.gave_up.append(change_id)

    def add_schedule_change_delivery(self, change_id, chat_id):
        change = next(c for c in self._changes if c['_id'] == change_id)
        change.setdefault('delivered_to', []).append(chat_id)


def make_change(change_id='change-1', group='АА-25-1', changed_at='2026-10-28T01:05:50Z'):
    return {'_id': change_id, 'group': group, 'changed_at': changed_at, 'notified': False}


class TestScheduleChangeNotifier(unittest.TestCase):
    def test_notifies_all_users_registered_for_changed_group_and_marks_processed(self):
        bot = FakeBot()
        storage = FakeStorage(
            changes=[make_change()],
            users_by_group={'АА-25-1': [{'chat_id': 111}, {'chat_id': 222}]},
        )

        ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW).process_pending()

        self.assertEqual([chat_id for chat_id, _ in bot.sent], [111, 222])
        self.assertTrue(all('АА-25-1' in text for _, text in bot.sent))
        self.assertEqual(storage.marked_notified, ['change-1'])

    def test_skips_users_without_chat_id(self):
        bot = FakeBot()
        storage = FakeStorage(
            changes=[make_change()],
            users_by_group={'АА-25-1': [{'chat_id': None}, {}]},
        )

        ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW).process_pending()

        self.assertEqual(bot.sent, [])
        self.assertEqual(storage.marked_notified, ['change-1'])

    def test_network_error_keeps_change_pending_and_retry_does_not_duplicate(self):
        users = {'АА-25-1': [{'chat_id': 111}, {'chat_id': 222}, {'chat_id': 333}]}
        storage = FakeStorage(changes=[make_change(), make_change('change-2', 'ББ-25-1')], users_by_group=users)
        bot = FakeBot(errors={222: NetworkError('Cannot connect to host api.telegram.org:443')})
        notifier = ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW)

        notifier.process_pending()

        self.assertEqual(bot.sent, [(111, bot.sent[0][1])])
        self.assertEqual(storage.marked_notified, [])

        bot.errors = {}
        notifier.process_pending()

        self.assertEqual([chat_id for chat_id, _ in bot.sent], [111, 222, 333])
        self.assertEqual(storage.marked_notified, ['change-1', 'change-2'])

    def test_blocked_user_does_not_block_the_change(self):
        bot = FakeBot(errors={111: BotBlocked('Forbidden: bot was blocked by the user')})
        storage = FakeStorage(
            changes=[make_change()],
            users_by_group={'АА-25-1': [{'chat_id': 111}, {'chat_id': 222}]},
        )

        ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW).process_pending()

        self.assertEqual([chat_id for chat_id, _ in bot.sent], [222])
        self.assertEqual(storage.marked_notified, ['change-1'])

    def test_gives_up_on_changes_older_than_a_day(self):
        bot = FakeBot()
        storage = FakeStorage(
            changes=[make_change(changed_at='2026-10-26T10:00:00Z')],
            users_by_group={'АА-25-1': [{'chat_id': 111}]},
        )

        ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW).process_pending()

        self.assertEqual(bot.sent, [])
        self.assertEqual(storage.gave_up, ['change-1'])


class TestTransferNotifications(unittest.TestCase):
    TRANSFER = {
        'name': 'Основы информационной безопасности', 'info': '( Лаб. раб. подгруппа 2 )', 'subgroup': 2,
        'time': '10:00', 'aud': ['Ж-312'], 'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00',
        'to_aud': ['В-104'], 'reason': 'инженерные каникулы',
    }

    def test_room_transfer_says_what_changed_and_skips_the_other_subgroup(self):
        bot = FakeBot()
        change = dict(make_change(group='ИСТб-24-2'), transfer=self.TRANSFER)
        storage = FakeStorage(
            changes=[change],
            users_by_group={'ИСТб-24-2': [{'chat_id': 1, 'subgroup': 1}, {'chat_id': 2, 'subgroup': 2},
                                         {'chat_id': 3, 'subgroup': 0}, {'chat_id': 4}]},
        )

        ScheduleChangeNotifier(bot=bot, storage=storage, now=lambda: NOW).process_pending()

        self.assertEqual([chat_id for chat_id, _ in bot.sent], [2, 3, 4])
        self.assertEqual(bot.sent[0][1], (
            '🔄 Перенос занятия, группа ИСТб-24-2\n'
            '👉 Основы информационной безопасности ( Лаб. раб. подгруппа 2 )\n'
            '📅 Ср, 28.10 в 10:00\n'
            '🏫 Аудитория: Ж-312 → В-104\n'
            'Причина: инженерные каникулы'
        ))
        self.assertEqual(storage.marked_notified, ['change-1'])

    def test_date_and_time_transfer_text(self):
        text = transfer_text('ИСТб-25-2', dict(self.TRANSFER, date='2026-10-23', to_date='2026-10-29',
                                                to_time='11:45', to_aud=['Ж-312'], reason=None))
        self.assertIn('📅 Пт, 23.10 в 10:00 → Чт, 29.10 в 11:45', text)
        self.assertIn('🏫 Аудитория: Ж-312', text)
        self.assertNotIn('→ Ж-312', text)

    def test_unmatched_transfer_text(self):
        text = transfer_text('ИСТб-24-2', {'name': '«Физика», перенос из В-208', 'info': '( Лекция )',
                                           'to_date': '2026-10-29', 'to_time': '11:45', 'to_aud': ['Б-118а']})
        self.assertIn('📅 Чт, 29.10 в 11:45', text)
        self.assertIn('🏫 Аудитория: Б-118а', text)


if __name__ == '__main__':
    unittest.main()
