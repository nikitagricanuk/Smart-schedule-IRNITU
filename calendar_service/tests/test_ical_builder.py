import unittest
from datetime import datetime

from unittest import mock

import pytz

import ical_builder

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


class TestTransfers(unittest.TestCase):
    def _calendar_events(self, lesson, day='среда'):
        from icalendar import Calendar
        now = TZ_IRKUTSK.localize(datetime(2026, 10, 27, 12, 0))
        with mock.patch.object(ical_builder, 'datetime', wraps=datetime) as fake_datetime:
            fake_datetime.now.side_effect = lambda tz=None: now if tz else now.replace(tzinfo=None)
            ics = ical_builder.build_calendar({'schedule': [{'day': day, 'lessons': [lesson]}]}, 'Тест', subgroup=1)
        return [c for c in Calendar.from_ical(ics).walk('VEVENT')]

    def test_room_transfer_overrides_that_occurrence_of_the_series(self):
        lesson = _lesson('Основы информационной безопасности', week='all', time='10:00',
                         info='( Лаб. раб. подгруппа 1 )', aud=['Ж-312'],
                         transfers=[{'date': '2026-10-28', 'to_date': '2026-10-28', 'to_time': '10:00',
                                     'to_aud': ['В-104'], 'reason': 'инженерные каникулы'}])
        events = self._calendar_events(lesson)
        self.assertEqual(len(events), 2)
        series, override = events
        self.assertIn('rrule', series)
        self.assertEqual(str(series['location']), 'Ж-312')
        self.assertEqual(override['uid'], series['uid'])
        self.assertEqual(override['recurrence-id'].dt.astimezone(TZ_IRKUTSK),
                         TZ_IRKUTSK.localize(datetime(2026, 10, 28, 10, 0)))
        self.assertEqual(override['dtstart'].dt.astimezone(TZ_IRKUTSK),
                         TZ_IRKUTSK.localize(datetime(2026, 10, 28, 10, 0)))
        self.assertEqual(str(override['location']), 'В-104')
        self.assertIn('Ж-312 → В-104', str(override['description']))

    def test_date_transfer_moves_the_occurrence(self):
        lesson = _lesson('Базы данных', week='all', time='08:15', aud=['В-202'],
                         transfers=[{'date': '2026-10-28', 'to_date': '2026-10-30', 'to_time': '11:45',
                                     'to_aud': ['Б-118а']}])
        override = self._calendar_events(lesson)[1]
        self.assertEqual(override['recurrence-id'].dt.astimezone(TZ_IRKUTSK),
                         TZ_IRKUTSK.localize(datetime(2026, 10, 28, 8, 15)))
        self.assertEqual(override['dtstart'].dt.astimezone(TZ_IRKUTSK),
                         TZ_IRKUTSK.localize(datetime(2026, 10, 30, 11, 45)))

    def test_one_off_with_date_field_and_other_subgroup_is_filtered(self):
        other_subgroup = _lesson('«Физика», перенос из В-208', week='even', time='10:00',
                                 info='( Лекция подгруппа 2 )', date='2026-10-28')
        self.assertEqual(self._calendar_events(other_subgroup), [])
        mine = dict(other_subgroup, info='( Лекция подгруппа 1 )')
        events = self._calendar_events(mine)
        self.assertEqual(len(events), 1)
        self.assertNotIn('rrule', events[0])
        self.assertEqual(events[0]['dtstart'].dt.astimezone(TZ_IRKUTSK).date(), datetime(2026, 10, 28).date())


if __name__ == '__main__':
    unittest.main()
