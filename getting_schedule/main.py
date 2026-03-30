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
from datetime import datetime, timezone

import json
import requests

# Задержка работы цикла (в часах).
GETTING_SCHEDULE_TIME_HOURS = float(os.environ.get('GETTING_SCHEDULE_TIME_HOURS')
                                    if os.environ.get('GETTING_SCHEDULE_TIME_HOURS')
                                    else 1) * 60 * 60
SCHEDULE_SOURCE = os.environ.get('SCHEDULE_SOURCE', 'istu_website').lower().strip()

mongo_storage = MongodbService().get_instance()


def _status_timestamp() -> str:
    return datetime.utcnow().isoformat(timespec='seconds') + 'Z'


def _update_runtime_status(state: str, stage: str, **extra):
    status = {
        'state': state,
        'stage': stage,
        'source': SCHEDULE_SOURCE,
    }
    status.update(extra)
    try:
        mongo_storage.update_runtime_status(**status)
    except Exception as error:
        logger.warning(f'Failed to save getting_schedule runtime status: {error}')


def _parse_status_datetime(value: str):
    if not value:
        return None
    try:
        normalized = value[:-1] + '+00:00' if value.endswith('Z') else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _can_use_cached_website_data() -> bool:
    status = mongo_storage.get_status('getting_schedule')
    if not status:
        return False

    last_success_at = _parse_status_datetime(status.get('last_success_at'))
    if not last_success_at:
        return False

    cache_age_sec = (datetime.now(timezone.utc) - last_success_at).total_seconds()
    if cache_age_sec >= GETTING_SCHEDULE_TIME_HOURS:
        return False

    required_collections = ['groups', 'schedule', 'prepods_schedule']
    missing_collections = [
        collection for collection in required_collections
        if not mongo_storage.collection_has_documents(collection)
    ]
    if missing_collections:
        logger.warning(
            f'Cached schedule status exists, but required collections are empty: {", ".join(missing_collections)}'
        )
        return False

    logger.info(
        f'Using cached website schedule data from {status.get("last_success_at")} '
        f'(age={round(cache_age_sec, 1)} sec); skip parsing on this start.'
    )
    return True


def _restore_cached_empty_schedule_docs(new_docs: list, collection: str, key_field: str) -> list:
    cached_docs = {
        doc.get(key_field): doc
        for doc in mongo_storage.get_data(collection)
        if doc.get(key_field)
    }
    restored_count = 0
    merged_docs = []

    for doc in new_docs:
        doc_key = doc.get(key_field)
        new_schedule = doc.get('schedule', [])
        cached_doc = cached_docs.get(doc_key)
        cached_schedule = cached_doc.get('schedule', []) if cached_doc else []

        if not new_schedule and cached_schedule:
            merged_docs.append(cached_doc)
            restored_count += 1
            logger.warning(
                f'{collection}: restored cached schedule for {key_field}="{doc_key}" '
                'because newly parsed schedule is empty'
            )
            continue

        merged_docs.append(doc)

    if restored_count:
        logger.warning(f'{collection}: restored {restored_count} cached schedules after empty parse results')

    return merged_docs


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

    _update_runtime_status(
        state='running',
        stage='starting_website_parser',
        started_at=_status_timestamp(),
    )

    def parser_progress_callback(stage: str, payload: dict):
        _update_runtime_status(
            state='running',
            stage=stage,
            **payload,
        )

    parser = ISTUScheduleParser(progress_callback=parser_progress_callback)
    data = parser.parse()
    data['schedule'] = _restore_cached_empty_schedule_docs(
        new_docs=data['schedule'],
        collection='schedule',
        key_field='group',
    )

    _update_runtime_status(
        state='running',
        stage='saving_collections',
        parsed_groups=len(data['schedule']),
        parsed_teachers=len(data['prepods_schedule']),
        parsed_auditories=len(data['auditories_schedule']),
    )

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
    finished_at = _status_timestamp()
    _update_runtime_status(
        state='success',
        stage='website_processing_completed',
        finished_at=finished_at,
        last_success_at=finished_at,
        last_duration_sec=round(end_time - start_time, 2),
        parsed_groups=len(data['schedule']),
        parsed_teachers=len(data['prepods_schedule']),
        parsed_auditories=len(data['auditories_schedule']),
    )
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
        cycle_started_at = _status_timestamp()
        cycle_failed = False

        _update_runtime_status(
            state='running',
            stage='cycle_started',
            cycle_started_at=cycle_started_at,
            cache_used=False,
        )

        try:
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
                if _can_use_cached_website_data():
                    _update_runtime_status(
                        state='success',
                        stage='using_cached_website_data',
                        cache_used=True,
                    )
                else:
                    processing_schedule_from_website()
        except Exception as e:
            cycle_failed = True
            _update_runtime_status(
                state='error',
                stage='schedule_processing_failed',
                last_error=str(e),
                cycle_started_at=cycle_started_at,
            )
            logger.error(f'Unexpected schedule processing error:\n{e}')
            traceback.print_exc()

        # Обновление базы экзаменов
        try:
            exam_update()
        except:
            traceback.print_exc()

        # Время окончания работы цикла.
        end_time = time.time()
        _update_runtime_status(
            state='idle',
            stage='waiting_for_next_cycle' if not cycle_failed else 'waiting_after_error',
            cycle_started_at=cycle_started_at,
            cycle_finished_at=_status_timestamp(),
            last_cycle_duration_sec=round(end_time - start_time, 2),
            next_run_in_hours=GETTING_SCHEDULE_TIME_HOURS / 60 / 60,
        )
        logger.info(f'Total operating time --- {end_time - start_time} seconds ---')

        # Задержка работы цикла (в часах).
        logger.info(f'Waiting for the next cycle. The waiting time: {GETTING_SCHEDULE_TIME_HOURS / 60 / 60} hours...\n')
        time.sleep(GETTING_SCHEDULE_TIME_HOURS)


# =====================================

if __name__ == '__main__':
    main()
