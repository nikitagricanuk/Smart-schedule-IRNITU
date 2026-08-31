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
          <li><a href="/raspisanie/podrazdelenie/664">Аспирантура</a></li>
          <li><a href="/raspisanie/podrazdelenie/1">Институт авиамашиностроения и транспорта</a></li>
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
        <div class="schd-kurs-block">
          <div class="schd-kurs-nuber">1 курс</div>
          <div class="schd-kurs-groups">
            <div class="schd-grp-item"><a href="/raspisanie/grup/111">АА-25-1</a></div>
            <div class="schd-grp-item"><a href="/raspisanie/grup/112">АА-25-2</a></div>
          </div>
        </div>
        <div class="schd-kurs-block">
          <div class="schd-kurs-nuber">2 курс</div>
          <div class="schd-kurs-groups">
            <div class="schd-grp-item"><a href="/raspisanie/grup/221">ББ-24-1</a></div>
          </div>
        </div>
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
        <h1>Группа АД-22-1</h1>
        <div class="sch-list-week">
          <div class="sch-list-day" data-params="{'date':'23.03.2026'}">
            <h2 class="sch-list-day-header">понедельник, 23 марта</h2>
            <div class="sch-list-item" data-params="{'time':'17:10'}">
              <div class="sch-list-item-time"><div class="sch-list-item-time-inner">17:10</div></div>
              <div class="sch-list-item-classes">
                <div class="sch-list-item-week week-even">
                  <div class="schcls-item schcls-card">
                    <div class="schcls-item-info">
                      <div class="schcls-item-name">Реконструкция автомобильных дорог</div>
                      <div class="schcls-item-distype type-2">практика</div>
                      <div class="schcls-item-prepod"><a href="/raspisanie/prepodavatel/947/">Волкова Е.В.</a></div>
                      <div class="schcls-item-group"><a href="/raspisanie/grup/473784/">АД-22-1</a></div>
                    </div>
                    <div class="schcls-item-aud"><a href="/raspisanie/aud/354/">Г-110б</a></div>
                  </div>
                </div>
                <div class="sch-list-item-week week-odd">
                  <div class="schcls-item schcls-card schcls-empty">свободно</div>
                </div>
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

    def test_parse_group_schedule_html_supports_week_all(self):
        day_name = schedule_tools.DAYS[2]
        html = f"""
        <h1>Группа AA-22-1</h1>
        <div class="sch-list-week">
          <div class="sch-list-day" data-params="{{'date':'24.03.2026'}}">
            <h2 class="sch-list-day-header">{day_name}, 24 test</h2>
            <div class="sch-list-item" data-params="{{'time':'18:45'}}">
              <div class="sch-list-item-time"><div class="sch-list-item-time-inner">18:45</div></div>
              <div class="sch-list-item-classes">
                <div class="sch-list-item-week week-all">
                  <div class="schcls-item schcls-card">
                    <div class="schcls-item-info">
                      <div class="schcls-item-name">Algorithms</div>
                      <div class="schcls-item-distype type-1">лекция</div>
                      <div class="schcls-item-prepod"><a href="/raspisanie/prepodavatel/123/">Ivanov I.I.</a></div>
                      <div class="schcls-item-group">
                        <a href="/raspisanie/grup/473784/">AA-22-1</a>
                        подгруппа 1
                      </div>
                    </div>
                    <div class="schcls-item-aud"><a href="/raspisanie/aud/200/">B-201</a></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        """

        group_name, schedule, events = parse_group_schedule_html(html=html, fallback_group_name="FALLBACK")
        self.assertEqual(group_name, "AA-22-1")
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0]["day"], day_name)
        self.assertEqual(len(schedule[0]["lessons"]), 1)
        self.assertEqual(schedule[0]["lessons"][0]["week"], "all")
        self.assertEqual(schedule[0]["lessons"][0]["info"], "( Лекция подгруппа 1 )")
        self.assertEqual(events[0]["name"], "Algorithms")

    def test_parse_group_schedule_html_supports_multiple_subgroup_cards(self):
        html = """
        <h1>Группа СМ-23-1</h1>
        <div class="sch-list-week">
          <div class="sch-list-day" data-params="{'date':'21.09.2026'}">
            <h2 class="sch-list-day-header">понедельник, 21 сентября</h2>
            <div class="sch-list-item" data-params="{'time':'08:15'}">
              <div class="sch-list-item-time"><div class="sch-list-item-time-inner">8:15</div></div>
              <div class="sch-list-item-classes">
                <div class="sch-list-item-week week-odd">
                  <div class="schcls-item schcls-card">
                    <div class="schcls-item-info">
                      <div class="schcls-item-name">Динамика полета самолета</div>
                      <div class="schcls-item-distype type-3">лабораторная работа</div>
                      <div class="schcls-item-prepod"><a href="/raspisanie/prepodavatel/1/">Кривель С.М.</a></div>
                      <div class="schcls-item-group">
                        <a href="/raspisanie/grup/478487/">СМ-23-1</a>
                        подгруппа 1
                      </div>
                    </div>
                    <div class="schcls-item-aud"><a href="/raspisanie/aud/337/">Д-013</a></div>
                  </div>
                  <div class="schcls-item schcls-card">
                    <div class="schcls-item-info">
                      <div class="schcls-item-name">Силовая установка</div>
                      <div class="schcls-item-distype type-3">лабораторная работа</div>
                      <div class="schcls-item-prepod"><a href="/raspisanie/prepodavatel/2/">Исаев А.И.</a></div>
                      <div class="schcls-item-group">
                        <a href="/raspisanie/grup/478487/">СМ-23-1</a>
                        подгруппа 2
                      </div>
                    </div>
                    <div class="schcls-item-aud"><a href="/raspisanie/aud/903/">Д-104</a></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        """

        group_name, schedule, events = parse_group_schedule_html(html=html, fallback_group_name="FALLBACK")
        self.assertEqual(group_name, "СМ-23-1")
        self.assertEqual(len(schedule[0]["lessons"]), 2)
        self.assertEqual(len(events), 2)
        subgroups = sorted(lesson["info"] for lesson in schedule[0]["lessons"])
        self.assertEqual(subgroups, ["( Лаб. раб. подгруппа 1 )", "( Лаб. раб. подгруппа 2 )"])


if __name__ == "__main__":
    unittest.main()
