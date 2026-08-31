import unittest

from schedule_change_notifier import ScheduleChangeNotifier


class FakeBot:
    def __init__(self):
        self.sent = []

    def send_message(self, chat_id, text):
        self.sent.append((chat_id, text))


class FakeStorage:
    def __init__(self, changes, users_by_group):
        self._changes = changes
        self._users_by_group = users_by_group
        self.marked_notified = []

    def get_pending_schedule_changes(self):
        return self._changes

    def get_tg_users_for_group(self, group):
        return self._users_by_group.get(group, [])

    def mark_schedule_change_notified(self, change_id):
        self.marked_notified.append(change_id)


class TestScheduleChangeNotifier(unittest.TestCase):
    def test_notifies_all_users_registered_for_changed_group_and_marks_processed(self):
        bot = FakeBot()
        storage = FakeStorage(
            changes=[{'_id': 'change-1', 'group': 'АА-25-1', 'notified': False}],
            users_by_group={'АА-25-1': [{'chat_id': 111}, {'chat_id': 222}]},
        )
        notifier = ScheduleChangeNotifier(bot=bot, storage=storage)

        for change in storage.get_pending_schedule_changes():
            notifier._notify_group(change['group'])
            storage.mark_schedule_change_notified(change['_id'])

        self.assertEqual([chat_id for chat_id, _ in bot.sent], [111, 222])
        self.assertTrue(all('АА-25-1' in text for _, text in bot.sent))
        self.assertEqual(storage.marked_notified, ['change-1'])

    def test_skips_users_without_chat_id(self):
        bot = FakeBot()
        storage = FakeStorage(
            changes=[],
            users_by_group={'АА-25-1': [{'chat_id': None}, {}]},
        )
        notifier = ScheduleChangeNotifier(bot=bot, storage=storage)

        notifier._notify_group('АА-25-1')

        self.assertEqual(bot.sent, [])


if __name__ == '__main__':
    unittest.main()
