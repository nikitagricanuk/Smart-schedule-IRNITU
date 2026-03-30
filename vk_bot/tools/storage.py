import os
from pymongo import MongoClient
from tools.logger import logger

MONGO_DB_ADDR = os.environ.get('MONGO_DB_ADDR')
MONGO_DB_PORT = os.environ.get('MONGO_DB_PORT')
MONGO_DB_DATABASE = os.environ.get('MONGO_DB_DATABASE')


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

    def get_data(self, collection) -> list:
        """возвращает список документов из указанной коллекции"""
        return list(self._db[collection].find())

    def save_data(self, collection, data: dict):
        """сохраняет документ в указанную коллекцию"""
        return self._db[collection].insert_one(data)

    def save_institutes(self, institutes: list):
        """сохраняет список институтов в коллекцию institutes"""
        return self._db.institutes.insert_many(institutes)

    def save_courses(self, courses: list):
        """сохраняет список курсов в коллекцию courses"""
        return self._db.courses.insert_many(courses)

    def save_groups(self, groups: list):
        """сохраняет список групп в коллекцию groups"""
        return self._db.groups.insert_many(groups)

    def get_institutes(self) -> list:
        """возвращает список институтов"""
        return list(self._db.institutes.find())

    def get_groups(self, institute: str, course: str) -> list:
        """возвращает список групп на определённом курсе в определеннои институте"""
        return list(self._db.groups.find(filter={'institute': {'$regex': f'{institute}*'}, 'course': course}))

    def get_search_list(self, search_words: str) -> list:
        """возвращает список групп по слову из поиска"""
        search_words = "".join(
            x for x in search_words if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-')
        if not search_words:
            return None
        return list(self._db.groups.find(filter={'name': {'$regex': f'.*{search_words}.*', "$options": '/i'}}))

    def get_search_list_prep(self, search_words: str) -> list:
        """возвращает список преподавателей по слову из поиска"""
        search_words = "".join(
            x for x in search_words if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-')
        if not search_words:
            return None
        return list(self._db.prepods_schedule.find(
            filter={'prep_short_name': {'$regex': f'.*{search_words}.*', "$options": '/i'}}))

    # Поиск по ФИО преподавателя или его части
    def get_register_list_prep(self, search_words: str) -> list:
        """возвращает список преподавателей по слову из поиска"""
        return list(self._db.prepods_schedule.find(
            filter={'prep': {'$regex': f"(^{search_words}\\s.*)|(.*\\s{search_words}\\s.*)|(.*\\s{search_words}$)",
                             "$options": '/i'}}))

    def get_courses(self, institute='') -> list:
        """возвращает список курсов у определённого института"""
        return list(self._db.courses.find(filter={'institute': {'$regex': f'{institute}*'}}))

    def get_prep(self, surname: str) -> list:
        """возвращает список ФИО всех преподавателей"""
        return list(self._db.prepods.find(filter={'prep': {'$regex': f'^{surname}$', "$options": '/i'}}))

    def get_schedule(self, group):
        """возвращает расписание группы"""
        schedule_doc = self._db.schedule.find_one(filter={'group': group})
        if not schedule_doc:
            logger.warning(f'Schedule document not found in Mongo for group="{group}"')
            return None
        if not schedule_doc.get('schedule'):
            logger.warning(f'Schedule document is empty in Mongo for group="{group}"')
        return schedule_doc

    def get_schedule_prep(self, group):
        """возвращает расписание преподавателя"""
        return self._db.prepods_schedule.find_one(filter={'prep': group})

    def get_schedule_aud(self, aud: str) -> list:
        """возвращает расписание преподавателя"""
        aud = "".join(x for x in aud if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-')
        if not aud:
            return []
        return list(self._db.auditories_schedule.find(filter={'aud': {'$regex': f'.*{aud}.*', "$options": '/i'}}))

    # ======================================== VK ======================================== #
    def get_vk_user(self, chat_id: int):
        """Получение пользователя VK"""
        return self._db.VK_users.find_one(filter={'chat_id': chat_id})

    def delete_vk_user_or_userdata(self, chat_id: int, delete_only_course: bool = False):
        """Удаление пользователя или курса VK пользователя  из базы данных"""
        if delete_only_course:
            return self._db.VK_users.update_one(filter={'chat_id': chat_id}, update={'$unset': {'course': ''}},
                                                upsert=True)
        return self._db.VK_users.delete_one(filter={'chat_id': chat_id})

    def save_or_update_vk_user(self, chat_id: int, institute='', course='', group='', notifications=0, reminders=[]):
        """сохраняет или изменяет данные пользователя (коллекция users)"""
        update = {'chat_id': chat_id, 'notifications': 0}
        if institute:
            update['institute'] = institute
        if course:
            update['course'] = course
        if group:
            update['group'] = group
        if notifications:
            update['notifications'] = notifications
        if reminders:
            update['reminders'] = reminders

        return self._db.VK_users.update_one(filter={'chat_id': chat_id}, update={'$set': update}, upsert=True)

    def save_vk_statistics(self, action: str, date: str, time: str):
        statistics = {
            'action': action,
            'date': date,
            'time': time
        }
        return self._db.vk_statistics.insert_one(statistics)

    def get_schedule_exam(self, group):
        """возвращает расписание экзаменов"""
        return self._db.exams_schedule.find_one(filter={'group': group})

    def get_users_for_script(self):
        """Вытаскиваем всех пользвателей из базы"""
        return self._db.VK_users.find({})

