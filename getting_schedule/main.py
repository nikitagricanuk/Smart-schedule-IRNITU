import traceback
import hashlib

from functions import postgre_storage
import data_conversion
from functions.mongo_storage import MongodbService
from functions.logger import logger
from functions.istu_website_parser import ISTUScheduleParser

from pymongo.errors import PyMongoError
import psycopg2

import time
import os

import json
import requests

# Задержка работы цикла (в часах).
GETTING_SCHEDULE_TIME_HOURS = float(os.environ.get('GETTING_SCHEDULE_TIME_HOURS')
                                    if os.environ.get('GETTING_SCHEDULE_TIME_HOURS')
                                    else 1) * 60 * 60
SCHEDULE_SOURCE = os.environ.get('SCHEDULE_SOURCE', 'istu_website').lower().strip()

mongo_storage = MongodbService().get_instance()


def _build_hash(data) -> str:
    """Считает контрольную сумму для набора документов."""
    data_json = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(data_json.encode('utf-8')).hexdigest()


def _save_collection_if_changed(
    hash_name: str,
    data: list,
    save_method,
    empty_method=None
):
    """Сохраняет коллекцию только если данные изменились."""
    data_hash = _build_hash(data)
    previous_hash = mongo_storage.get_hash(hash_name=hash_name)
    if previous_hash == data_hash:
        logger.info(f'{hash_name}: no changes detected')
        return

    if data:
        save_method(data)
    elif empty_method:
        empty_method()

    mongo_storage.save_hash(hash_name=hash_name, value=data_hash)
    logger.info(f'{hash_name}: data updated')


def processing_schedule_from_website():
    """Обработка расписания с официального сайта ИРНИТУ."""
    logger.info('Start processing_schedule_from_website...')
    start_time = time.time()

    parser = ISTUScheduleParser()
    data = parser.parse()

    _save_collection_if_changed(
        hash_name='institutes',
        data=sorted(data['institutes'], key=lambda x: x['name']),
        save_method=mongo_storage.save_institutes,
    )
    _save_collection_if_changed(
        hash_name='groups',
        data=sorted(data['groups'], key=lambda x: x['name']),
        save_method=mongo_storage.save_groups,
    )
    _save_collection_if_changed(
        hash_name='courses',
        data=sorted(data['courses'], key=lambda x: (x['institute'], x['name'])),
        save_method=mongo_storage.save_courses,
    )
    _save_collection_if_changed(
        hash_name='prepods',
        data=sorted(data['prepods'], key=lambda x: (x['prep'], x['prep_id'])),
        save_method=mongo_storage.save_teachers,
    )
    _save_collection_if_changed(
        hash_name='schedule',
        data=sorted(data['schedule'], key=lambda x: x['group']),
        save_method=mongo_storage.save_schedule,
        empty_method=mongo_storage.delete_schedule,
    )
    _save_collection_if_changed(
        hash_name='prepods_schedule',
        data=sorted(data['prepods_schedule'], key=lambda x: (x['prep'], x['pg_id'])),
        save_method=mongo_storage.save_teachers_schedule,
        empty_method=mongo_storage.delete_teachers_schedule,
    )
    _save_collection_if_changed(
        hash_name='auditories_schedule',
        data=sorted(data['auditories_schedule'], key=lambda x: x['aud']),
        save_method=mongo_storage.save_auditories_schedule,
        empty_method=mongo_storage.delete_auditories_schedule,
    )

    end_time = time.time()
    logger.info(f'Processing_schedule_from_website successful. Operation time: {end_time - start_time} seconds.')


def processing_institutes():
    """Обработка институтов"""
    logger.info('Start processing_institutes...')
    start_time = time.time()

    try:
        # Получаем данные.
        pg_institutes = postgre_storage.get_institutes()
        # Преобразуем данные в нужный формат.
        mongo_institutes = sorted(data_conversion.convert_institutes(pg_institutes),
                                  key=lambda x: x['name'])  # Сортируем массив
        # Сохраняем данные.
        mongo_storage.save_institutes(mongo_institutes)

        end_time = time.time()
        logger.info(f'Processing_institutes successful. Operation time: {end_time - start_time} seconds.')

    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_institutes error:\n{e}')


def processing_groups_and_courses():
    """Обработка групп и курсов"""
    logger.info('Start processing_groups...')
    start_time_groups = time.time()
    try:
        # Группы
        pg_groups = postgre_storage.get_groups()
        mongo_groups = sorted(data_conversion.convert_groups(pg_groups),
                              key=lambda x: x['name'])
        mongo_storage.save_groups(mongo_groups)  # Сохраняем группы

        end_time_groups = time.time()
        logger.info(f'Processing_groups successful. Operation time: {end_time_groups - start_time_groups} seconds.')
        logger.info('Start processing_courses...')
        start_time_courses = time.time()

        try:
            # Курсы
            mongo_courses = sorted(data_conversion.convert_courses(mongo_groups),
                                   key=lambda x: x['name'])
            mongo_storage.save_courses(mongo_courses)  # Сохраняем курсы
        except PyMongoError as e:
            logger.error(f'Mongo error:\n{e}')
        except Exception as e:
            logger.error(f'convert_courses error:\n{e}')

        end_time_courses = time.time()
        logger.info(f'Processing_courses successful. Operation time: {end_time_courses - start_time_courses} seconds.')

    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_groups error:\n{e}')


def processing_teachers():
    """Обработка преподавателей"""
    logger.info('Start processing_teachers...')
    start_time = time.time()

    try:
        pg_teachers = postgre_storage.get_teachers()
        mongo_teachers = sorted(data_conversion.convert_teachers(pg_teachers),
                                key=lambda x: x['prep'])  # Сортируем массив
        mongo_storage.save_teachers(mongo_teachers)

        end_time = time.time()
        logger.info(f'Processing_teachers successful. Operation time: {end_time - start_time} seconds.')

    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_teachers error:\n{e}')


def processing_schedule():
    """Обработка расписания"""
    logger.info('Start processing_schedule...')
    start_time1 = time.time()
    try:
        pg_schedule = postgre_storage.get_schedule()
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
        return

    # Расписание студентов
    try:
        mongo_schedule = data_conversion.convert_schedule(pg_schedule)

        if mongo_schedule:
            mongo_storage.save_schedule(mongo_schedule)
        else:
            mongo_storage.delete_schedule()

        end_time1 = time.time()
        logger.info(f'Processing_schedule successful. Operation time: {end_time1 - start_time1} seconds.')

    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_schedule error:\n{e}')

    # Расписание преподавателей
    logger.info('Start processing_teachers_schedule...')
    start_time2 = time.time()
    try:
        mongo_teachers_schedule = data_conversion.convert_teachers_schedule(pg_schedule)

        if mongo_teachers_schedule:
            mongo_storage.save_teachers_schedule(mongo_teachers_schedule)
        else:
            mongo_storage.delete_teachers_schedule()

        end_time2 = time.time()
        logger.info(f'Processing_teachers_schedule successful. Operation time: {end_time2 - start_time2} seconds.')
    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_teachers_schedule error:\n{e}')

    # Расписание аудиторий
    logger.info('Start processing_auditories_schedule...')
    start_time3 = time.time()
    try:
        mongo_auditories_schedule = data_conversion.convert_auditories_schedule(pg_schedule)

        if mongo_auditories_schedule:
            mongo_storage.save_auditories_schedule(mongo_auditories_schedule)
        else:
            mongo_storage.delete_auditories_schedule()

        end_time3 = time.time()
        logger.info(f'Processing_auditories_schedule successful. Operation time: {end_time3 - start_time3} seconds.')
    except PyMongoError as e:
        logger.error(f'Mongo error:\n{e}')
    except psycopg2.OperationalError as e:
        logger.error(f'Postgre error:\n{e}')
    except Exception as e:
        logger.error(f'convert_auditories_schedule error:\n{e}')


def exam_update():
    logger.info('Start processing_exams_schedule...')

    JSON_EXAMS = os.environ.get('EXAMS_API')
    if not JSON_EXAMS:
        logger.warning('EXAMS_API is not set, skip processing_exams_schedule')
        return

    try:
        response = requests.get(JSON_EXAMS, timeout=20)
        response.raise_for_status()
        json_data = json.loads(response.text)
        schedule_exams = [{'group': a, 'exams': d} for a, d in json_data.items()]
        if schedule_exams:
            _save_collection_if_changed(
                hash_name='exams_schedule',
                data=sorted(schedule_exams, key=lambda x: x['group']),
                save_method=mongo_storage.save_schedule_exam,
            )
        else:
            logger.warning('EXAMS_API returned empty payload')
        logger.info('End processing_exams_schedule...')

    except requests.exceptions.RequestException as error:
        logger.error(f"Error processing_exams_schedule: {error}")


def main():
    while True:
        # Время начала работы цикла.
        start_time = time.time()

        if SCHEDULE_SOURCE == 'postgres' and os.environ.get('PG_DB_HOST'):
            # Институты
            processing_institutes()

            # Группы и курсы
            processing_groups_and_courses()

            # Преподаватели
            processing_teachers()

            # Расписание
            processing_schedule()
        else:
            if SCHEDULE_SOURCE == 'postgres' and not os.environ.get('PG_DB_HOST'):
                logger.warning('SCHEDULE_SOURCE=postgres, but PG_DB_HOST is empty. Fallback to ISTU website parser.')
            processing_schedule_from_website()

        # Обновление базы экзаменов
        try:
            exam_update()
        except:
            traceback.print_exc()

        # Время окончания работы цикла.
        end_time = time.time()
        logger.info(f'Total operating time --- {end_time - start_time} seconds ---')

        # Задержка работы цикла (в часах).
        logger.info(f'Waiting for the next cycle. The waiting time: {GETTING_SCHEDULE_TIME_HOURS / 60 / 60} hours...\n')
        time.sleep(GETTING_SCHEDULE_TIME_HOURS)


# =====================================

if __name__ == '__main__':
    main()
