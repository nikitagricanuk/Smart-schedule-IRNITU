import unittest
from datetime import datetime

import pytz

from ical_builder import TZ_IRKUTSK, _build_event, week0_monday

WEEK_START = TZ_IRKUTSK.localize(datetime(2026, 9, 14))
HORIZON_END = TZ_IRKUTSK.localize(datetime(2027, 8, 31, 23, 59, 59))
MONDAY = week0_monday(WEEK_START)


def _lesson(name, week='even', time='08:15', **extra):
    lesson = {'name': name, 'week': week, 'time': time, 'aud': [], 'prep': [], 'groups': []}
    lesson.update(extra)
    return lesson


class TestOneTimeTransferDetection(unittest.TestCase):
    def test_razovy_perenos_prefix_with_destination_date_is_one_time_and_non_recurring(self):
        lesson = _lesson('Разовый перенос «Программирование», на 2026.09.19')
        event = _build_event('понедельник', lesson, MONDAY, WEEK_START, HORIZON_END)

        self.assertIsNotNone(event)
        self.assertNotIn('rrule', event)
        dtstart = event['dtstart'].dt.astimezone(TZ_IRKUTSK)
        self.assertEqual(dtstart.date(), datetime(2026, 9, 19).date())

    def test_perenos_s_source_date_variant_is_one_time_at_card_day_not_embedded_date(self):
        # This is the real, moved occurrence card ISTU renders at its new day/time;
        # the embedded date is the OLD date it moved FROM, not this occurrence's date.
        lesson = _lesson(
            '2-ая подгруппа «Основы мобильной разработки», перенос с 2026.09.19',
            week='even',
        )
        # понедельник, чётная неделя -> occurrence falls on 2026-09-14 (WEEK_START),
        # the same day/week the normal day_name+parity logic would compute.
        event = _build_event('понедельник', lesson, MONDAY, WEEK_START, HORIZON_END)

        self.assertIsNotNone(event)
        self.assertNotIn('rrule', event)
        dtstart = event['dtstart'].dt.astimezone(TZ_IRKUTSK)
        # Occurrence date must be the card's own computed day, NOT the embedded
        # 2026.09.19 source date.
        self.assertEqual(dtstart.date(), WEEK_START.date())
        self.assertNotEqual(dtstart.date(), datetime(2026, 9, 19).date())

    def test_room_transfer_without_date_stays_recurring(self):
        lesson = _lesson('Физика, перенос из В-208', week='even')
        event = _build_event('понедельник', lesson, MONDAY, WEEK_START, HORIZON_END)

        self.assertIsNotNone(event)
        self.assertIn('rrule', event)


if __name__ == '__main__':
    unittest.main()
