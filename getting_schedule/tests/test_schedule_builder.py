import unittest

from functions.schedule_builder import _build_info, build_teacher_and_auditory_schedules


class TestScheduleBuilder(unittest.TestCase):
    def test_build_info_normalizes_lesson_type(self):
        self.assertEqual(_build_info("лекция", None), "( Лекция )")
        self.assertEqual(_build_info("практика", "1"), "( Практ. подгруппа 1 )")
        self.assertEqual(_build_info("лабораторная", None), "( Лаб. раб. )")

    def test_build_teacher_and_auditory_schedules(self):
        events = [
            {
                "day": "понедельник",
                "time": "17:10",
                "week": "even",
                "name": "Реконструкция автомобильных дорог",
                "info": "( Практ. )",
                "groups": ["АД-22-1"],
                "aud": ["Г-110б"],
                "prep_meta": [(947, "Волкова Е.В.")],
            },
            {
                "day": "понедельник",
                "time": "17:10",
                "week": "odd",
                "name": "свободно",
                "info": "",
                "groups": ["АД-22-1"],
                "aud": [],
                "prep_meta": [],
            },
        ]

        teacher_docs, aud_docs, prepods = build_teacher_and_auditory_schedules(events)
        self.assertEqual(len(teacher_docs), 1)
        self.assertEqual(teacher_docs[0]["prep"], "Волкова Е.В.")
        self.assertEqual(teacher_docs[0]["pg_id"], 947)
        self.assertEqual(teacher_docs[0]["schedule"][0]["lessons"][0]["groups"], ["АД-22-1"])
        self.assertEqual(len(aud_docs), 1)
        self.assertEqual(aud_docs[0]["aud"], "Г-110б")
        self.assertEqual(prepods, [{"prep": "Волкова Е.В.", "prep_short_name": "Волкова Е.В.", "prep_id": 947}])


if __name__ == "__main__":
    unittest.main()
