import os
import re
from contextlib import closing
from datetime import datetime

import pendulum as pendulum
import psycopg2
from psycopg2.extras import DictCursor

from data_conversion import TIME_ZONE, get_week_even

PG_DB_DATABASE = os.environ.get('PG_DB_DATABASE', default='schedule')
PG_DB_USER = os.environ.get('PG_DB_USER')
PG_DB_PASSWORD = os.environ.get('PG_DB_PASSWORD')
PG_DB_HOST = os.environ.get('PG_DB_HOST')
PG_DB_PORT = os.environ.get('PG_DB_PORT', default='5432')

db_params = {
    'database': PG_DB_DATABASE,
    'user': PG_DB_USER,
    'password': PG_DB_PASSWORD,
    'host': PG_DB_HOST,
    'port': PG_DB_PORT
}


def get_institutes() -> list:
    """Получение институтов из PostgreSQL"""
    with closing(psycopg2.connect(**db_params)) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT DISTINCT faculty_title from real_groups WHERE coalesce(faculty_title, '') != '' WHERE is_active = True")
            rows = cursor.fetchall()
            institutes = [dict(institute) for institute in rows]
            return institutes


def get_groups() -> list:
    """Получение групп из PostgreSQL"""
    with closing(psycopg2.connect(**db_params)) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # Вместо id института подставляется сразу название.
            cursor.execute("SELECT obozn, kurs, faculty_title as fac FROM real_groups WHERE is_active = True")
            rows = cursor.fetchall()
            groups = [dict(group) for group in rows]
            return groups


def get_teachers() -> list:
    """Получение преподавателей из PostgreSQL"""
    with closing(psycopg2.connect(**db_params)) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # Вместо id института подставляется сразу название.
            cursor.execute("SELECT "
                           "preps as prep, "
                           "prep as prep_short_name, "
                           "id_61 as prep_id "
                           "from prepods")
            rows = cursor.fetchall()
            teachers = [dict(teacher) for teacher in rows]
            return teachers


def get_schedule() -> list:
    """Получение расписания групп из PostgreSQL"""
    date_now = datetime.now(TIME_ZONE)
    start_of_first_week = pendulum.instance(date_now).start_of("week")
    start_of_second_week = pendulum.instance(start_of_first_week).add(weeks=1)

    is_even = get_week_even(date_now)
    if is_even == 1:
        odd_week = start_of_second_week
        even_week = start_of_first_week
    else:
        even_week = start_of_second_week
        odd_week = start_of_first_week

    query = """
SELECT coalesce(g.obozn, '') as obozn,
       dbeg,
       dend,
       begtime,
       everyweek,
       preps, prep_short_name,
       prep_id, auditories_verbose,
       case when dbeg = '{odd_week:%Y-%m-%d}' then (day - 1) % 7 + 1 when  dbeg = '{even_week:%Y-%m-%d}' then (day - 1) % 7 + 8 end as day,
       nt,
       title, ngroup
FROM (
           SELECT unnest(groups)   group_id,
                  dbeg,
                  dend,
                  vacpara.begtime,
                  everyweek,
                  prepods.preps,
                  prepods.prep  as prep_short_name,
                  prepods.id_61 as prep_id,
                  CASE
                      when -1 = any (s.auditories) then 'онлайн'
                      else auditories.obozn
                      end
                                as auditories_verbose,
                  day,
                  nt,
                  coalesce(disciplines.title, discipline_verbose) as title,
                  ngroup
           from schedule_v2 s
                    join vacpara on s.para = vacpara.id_66
                    left join prepods on prepods.id_61 = any (s.teachers)
                    left join disciplines on s.discipline = disciplines.id
                    left join auditories on auditories.id_60 = any (s.auditories)
) t
LEFT JOIN real_groups g ON t.group_id = g.id_7 and g.is_active=TRUE
WHERE ((dbeg = '{odd_week:%Y-%m-%d}' and (everyweek = 2 or everyweek = 1 and day <= 7)) -- нечетная
    or (dbeg = '{even_week:%Y-%m-%d}' and (everyweek = 2 or everyweek = 1 and day > 7))) -- четная
ORDER BY group_id
            """.format(odd_week=odd_week, even_week=even_week)

    with closing(psycopg2.connect(**db_params)) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()
            groups = [dict(group) for group in rows]

            for group in groups:
                group['title'] = re.sub('<[^<]+?>', '', group['title'])

            return groups
