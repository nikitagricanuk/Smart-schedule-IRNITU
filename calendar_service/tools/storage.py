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

    def get_schedule(self, group_name: str):
        """Возвращает документ расписания группы."""
        return self._db.schedule.find_one(filter={'group': group_name})

    def get_schedule_prep(self, prep_name: str):
        """Возвращает документ расписания преподавателя."""
        return self._db.prepods_schedule.find_one(filter={'prep': prep_name})
