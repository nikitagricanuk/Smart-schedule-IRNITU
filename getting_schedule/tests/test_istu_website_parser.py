import unittest

from functions import schedule_tools
from functions.istu_website_parser import (
    build_teacher_and_auditory_schedules,
    parse_group_schedule_html,
    parse_groups_html,
    parse_subdivisions_html,
)


class TestIstuWebsiteParser(unittest.TestCase):
    def test_parse_subdivisions_html(self):
        html = """
        <ul>
          <li><a href="?subdiv=664">Аспирантура</a></li>
          <li><a href="?subdiv=1">Институт авиамашиностроения и транспорта</a></li>
        </ul>
        """

        result = parse_subdivisions_html(html)
        expected = [
            {"subdiv_id": 664, "institute": "Аспирантура"},
            {"subdiv_id": 1, "institute": "Институт авиамашиностроения и транспорта"},
        ]
        self.assertEqual(result, expected)

    def test_parse_groups_html(self):
        html = """
        <ul class="kurs-list">
          <li>Курс 1
            <ul>
              <li><a href="?group=111">АА-25-1</a></li>
              <li><a href="?group=112">АА-25-2</a></li>
            </ul>
          </li>
          <li>Курс 2
            <ul>
              <li><a href="?group=221">ББ-24-1</a></li>
            </ul>
          </li>
        </ul>
        """

        result = parse_groups_html(html=html, institute="Тестовый институт")
        expected = [
            {"group_id": 111, "name": "АА-25-1", "course": "1 курс", "institute": "Тестовый институт"},
            {"group_id": 112, "name": "АА-25-2", "course": "1 курс", "institute": "Тестовый институт"},
            {"group_id": 221, "name": "ББ-24-1", "course": "2 курс", "institute": "Тестовый институт"},
        ]
        self.assertEqual(result, expected)

    def test_parse_group_schedule_html_and_derived_collections(self):
        html = """
        <div class="alert alert-info">
          <p>группа: <b>АД-22-1</b></p>
        </div>
        <div class="full-odd-week">
          <h3 class="day-heading">понедельник, 23 марта </h3>
          <div class="class-lines">
            <div class="class-line-item">
              <div class="class-tails">
                <div class="class-time">17:10</div>
                <div class="class-tail class-even-week">
                  <div class="class-info">практика <a href="?prep=947">Волкова Е.В.</a></div>
                  <div class="class-pred">Реконструкция автомобильных дорог</div>
                  <div class="class-info"><a href="?group=473784">АД-22-1</a></div>
                  <div class="class-aud"><a href="?aud=354">Г-110б</a></div>
                </div>
                <div class="class-tail class-odd-week">свободно</div>
              </div>
            </div>
          </div>
        </div>
        """

        group_name, schedule, events = parse_group_schedule_html(html=html, fallback_group_name="FALLBACK")
        self.assertEqual(group_name, "АД-22-1")
        self.assertEqual(schedule[0]["day"], "понедельник")
        self.assertEqual(len(schedule[0]["lessons"]), 2)

        even_lessons = [lesson for lesson in schedule[0]["lessons"] if lesson["week"] == "even"]
        self.assertEqual(len(even_lessons), 1)
        self.assertEqual(even_lessons[0]["name"], "Реконструкция автомобильных дорог")
        self.assertEqual(even_lessons[0]["prep"], ["Волкова Е.В."])
        self.assertEqual(even_lessons[0]["aud"], ["Г-110б"])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["name"], "Реконструкция автомобильных дорог")

        teacher_docs, aud_docs, prepods = build_teacher_and_auditory_schedules(events)
        self.assertEqual(len(teacher_docs), 1)
        self.assertEqual(teacher_docs[0]["prep"], "Волкова Е.В.")
        self.assertEqual(len(aud_docs), 1)
        self.assertEqual(aud_docs[0]["aud"], "Г-110б")
        self.assertEqual(len(prepods), 1)
        self.assertEqual(prepods[0]["prep"], "Волкова Е.В.")

    def test_parse_group_schedule_html_supports_even_week_wrapper(self):
        day_name = schedule_tools.DAYS[2]
        html = f"""
        <div class="full-even-week">
          <h3 class="day-heading">{day_name}, 24 test </h3>
          <div class="class-lines">
            <div class="class-line-item">
              <div class="class-tails">
                <div class="class-time">18:45</div>
                <div class="class-tail class-odd-week">
                  <div class="class-info">lecture <a href="?prep=123">Ivanov I.I.</a></div>
                  <div class="class-pred">Algorithms</div>
                  <div class="class-info"><a href="?group=473784">AA-22-1</a></div>
                  <div class="class-aud"><a href="?aud=200">B-201</a></div>
                </div>
              </div>
            </div>
          </div>
        </div>
        """

        group_name, schedule, events = parse_group_schedule_html(html=html, fallback_group_name="FALLBACK")
        self.assertEqual(group_name, "FALLBACK")
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0]["day"], day_name)
        self.assertEqual(len(schedule[0]["lessons"]), 1)
        self.assertEqual(schedule[0]["lessons"][0]["week"], "odd")
        self.assertEqual(events[0]["name"], "Algorithms")


if __name__ == "__main__":
    unittest.main()
