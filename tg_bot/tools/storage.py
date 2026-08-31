import os
import re

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorClient

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
        self._client = AsyncIOMotorClient(f'mongodb://{MONGO_DB_ADDR}:{MONGO_DB_PORT}')
        self._db = self._client[MONGO_DB_DATABASE]

    @staticmethod
    def _to_object_id(value):
        if value is None:
            return None
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError, ValueError):
            return None

    async def get_data(self, collection) -> list:
        return await self._db[collection].find().to_list(length=None)

    async def save_data(self, collection, data: dict):
        return await self._db[collection].insert_one(data)

    async def save_institutes(self, institutes: list):
        return await self._db.institutes.insert_many(institutes)

    async def save_courses(self, courses: list):
        return await self._db.courses.insert_many(courses)

    async def save_groups(self, groups: list):
        return await self._db.groups.insert_many(groups)

    async def get_institutes(self) -> list:
        return await self._db.institutes.find().to_list(length=None)

    async def get_institute_by_id(self, institute_id):
        object_id = self._to_object_id(institute_id)
        if object_id is None:
            return None
        return await self._db.institutes.find_one(filter={'_id': object_id})

    async def get_search_list(self, search_words: str) -> list:
        search_words = "".join(
            x for x in search_words if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-'
        )
        if not search_words:
            return []
        return await self._db.groups.find(
            filter={'name': {'$regex': f'.*{search_words}.*', "$options": 'i'}}
        ).to_list(length=None)

    async def get_search_list_prep(self, search_words: str) -> list:
        search_words = "".join(
            x for x in search_words if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-'
        )
        if not search_words:
            return []
        return await self._db.prepods_schedule.find(
            filter={'prep_short_name': {'$regex': f'.*{search_words}.*', "$options": 'i'}}
        ).to_list(length=None)

    async def get_prep(self, surname: str) -> list:
        return await self._db.prepods.find(
            filter={'prep': {'$regex': f'^{surname}$', "$options": 'i'}}
        ).to_list(length=None)

    async def get_prep_for_id(self, prep_id: int):
        return await self._db.prepods_schedule.find_one(filter={'pg_id': prep_id})

    async def get_courses(self, institute='') -> list:
        if not institute:
            return []
        institute_prefix = re.escape(institute.strip())
        return await self._db.courses.find(
            filter={'institute': {'$regex': f'^{institute_prefix}', '$options': 'i'}}
        ).to_list(length=None)

    async def get_course_by_id(self, course_id):
        object_id = self._to_object_id(course_id)
        if object_id is None:
            return None
        return await self._db.courses.find_one(filter={'_id': object_id})

    async def get_groups(self, institute: str, course: str) -> list:
        if not institute or not course:
            return []
        institute_prefix = re.escape(institute.strip())
        return await self._db.groups.find(
            filter={'institute': {'$regex': f'^{institute_prefix}', '$options': 'i'}, 'course': course}
        ).to_list(length=None)

    async def get_group_by_id(self, group_id):
        object_id = self._to_object_id(group_id)
        if object_id is None:
            return None
        return await self._db.groups.find_one(filter={'_id': object_id})

    async def get_register_list_prep(self, search_words: str) -> list:
        return await self._db.prepods_schedule.find(
            filter={'prep': {'$regex': f"(^{search_words}\\s.*)|(.*\\s{search_words}\\s.*)|(.*\\s{search_words}$)",
                             "$options": 'i'}}
        ).to_list(length=None)

    async def get_schedule_aud(self, aud: str) -> list:
        aud = "".join(x for x in aud if x.isalpha() or x.isdigit() or x.isspace() or x == '.' or x == '-')
        if not aud:
            return []
        return await self._db.auditories_schedule.find(
            filter={'aud': {'$regex': f'.*{aud}.*', "$options": 'i'}}
        ).to_list(length=None)

    async def get_schedule_prep(self, group):
        return await self._db.prepods_schedule.find_one(filter={'prep': group})

    async def save_or_update_user(self, chat_id: int, institute='', course='', group='', notifications=0, reminders=None):
        reminders = reminders or []
        update = {'chat_id': chat_id, 'notifications': 0, 'reminders': {}}
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
        return await self._db.users.update_one(filter={'chat_id': chat_id}, update={'$set': update}, upsert=True)

    async def get_user(self, chat_id: int):
        return await self._db.users.find_one(filter={'chat_id': chat_id})

    async def delete_user_or_userdata(self, chat_id: int, delete_only_course: bool = False):
        if delete_only_course:
            return await self._db.users.update_one(
                filter={'chat_id': chat_id}, update={'$unset': {'course': ''}}, upsert=True
            )
        return await self._db.users.delete_one(filter={'chat_id': chat_id})

    async def get_schedule(self, group):
        return await self._db.schedule.find_one(filter={'group': group})

    async def save_statistics(self, action: str, date: str, time: str):
        statistics = {'action': action, 'date': date, 'time': time}
        return await self._db.tg_statistics.insert_one(statistics)

    async def get_schedule_exam(self, group):
        return await self._db.exams_schedule.find_one(filter={'group': group})

    async def get_users_for_script(self):
        return await self._db.users.find({}).to_list(length=None)
