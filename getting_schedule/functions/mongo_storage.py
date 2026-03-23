import os
from pymongo import MongoClient

MONGO_DB_ADDR = os.environ.get('MONGO_DB_ADDR', default='localhost')
MONGO_DB_PORT = os.environ.get('MONGO_DB_PORT', default=27017)
MONGO_DB_DATABASE = os.environ.get('MONGO_DB_DATABASE', default='Smart_schedule_IRNITU')


class MongodbService(object):
    _instance = None
    _client = None
    _db = None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls.__init__(cls._instance, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self._client = MongoClient(f'mongodb://{MONGO_DB_ADDR}:{MONGO_DB_PORT}')
        self._db = self._client[MONGO_DB_DATABASE]

    def save_institutes(self, institutes: list):
        """Сохраняет список институтов в коллекцию institutes"""
        self._db.institutes.drop()  # очищаем старые записи в коллекции
        return self._db.institutes.insert_many(institutes)

    def save_courses(self, courses: list):
        """Сохраняет список курсов в коллекцию courses"""
        self._db.courses.drop()  # очищаем старые записи в коллекции
        return self._db.courses.insert_many(courses)

    def save_groups(self, groups: list):
        """Сохраняет список групп в коллекцию groups"""
        self._db.groups.drop()  # очищаем старые записи в коллекции
        return self._db.groups.insert_many(groups)

    def save_schedule(self, schedule: list):
        """Сохраняет расписание в коллекцию schedule"""
        self._db.schedule.drop()  # очищаем старые записи в коллекции
        return self._db.schedule.insert_many(schedule)

    def delete_schedule(self):
        """Удаляем расписание (очищаем коллекцию schedule)"""
        return self._db.schedule.drop()

    def save_teachers(self, groups: list):
        """Сохраняет список преподавателей в коллекцию prepods"""
        self._db.prepods.drop()  # очищаем старые записи в коллекции
        return self._db.prepods.insert_many(groups)

    def save_teachers_schedule(self, schedule: list):
        """Сохраняет расписание преподавателей в коллекцию prepods_schedule"""
        self._db.prepods_schedule.drop()  # очищаем старые записи в коллекции
        return self._db.prepods_schedule.insert_many(schedule)

    def delete_teachers_schedule(self):
        """Удаляем расписание преподавателей (очищаем коллекцию prepods_schedule)"""
        return self._db.prepods_schedule.drop()


    def save_auditories_schedule(self, schedule: list):
        """Сохраняет расписание аудиторий в коллекцию auditories_schedule"""
        self._db.auditories_schedule.drop()  # очищаем старые записи в коллекции
        return self._db.auditories_schedule.insert_many(schedule)

    def delete_auditories_schedule(self):
        """Удаляем расписание аудиторий (очищаем коллекцию auditories_schedule)"""
        return self._db.auditories_schedule.drop()


    def save_schedule_exam(self, exam):
        """записывает расписание экзаменов"""
        self._db.exams_schedule.drop()
        return self._db.exams_schedule.insert_many(exam)


    def save_status(self, date, time, getting_schedule_time_hours):
        """Сохраняет время последнего обращения к PostgreSQL"""
        status = {
            'name': 'getting_schedule',
            'date': date,
            'time': time,
            'getting_schedule_time_hours': getting_schedule_time_hours
        }

        return self._db.status.update_one(filter={'name': 'getting_schedule'}, update={'$set': status}, upsert=True)

    def get_status(self, name: str):
        """Возвращает документ статуса по имени."""
        return self._db.status.find_one(filter={'name': name})

    def save_hash(self, hash_name: str, value: str):
        """Сохраняет контрольную сумму коллекции."""
        status = {
            'name': f'hash_{hash_name}',
            'value': value
        }
        return self._db.status.update_one(filter={'name': status['name']}, update={'$set': status}, upsert=True)

    def get_hash(self, hash_name: str):
        """Возвращает контрольную сумму коллекции."""
        status = self.get_status(name=f'hash_{hash_name}')
        if not status:
            return None
        return status.get('value')
