import os

from pymongo import MongoClient

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

    def get_users_with_reminders_tg(self):
        return list(self._db.users.find(filter={'notifications': {'$ne': 0}}))

    def get_users_with_reminders_vk(self):
        return list(self._db.VK_users.find(filter={'notifications': {'$ne': 0}}))

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

    def save_or_update_tg_user(self, chat_id: int, institute='', course='', group='', notifications=0, reminders=[]):
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

        return self._db.users.update_one(filter={'chat_id': chat_id}, update={'$set': update}, upsert=True)

    def get_schedule(self, group):
        """возвращает расписание группы"""
        return self._db.schedule.find_one(filter={'group': group})

    def get_pending_schedule_changes(self) -> list:
        """возвращает необработанные записи об изменении расписания"""
        return list(self._db.schedule_changes.find(filter={'notified': False}))

    def mark_schedule_change_notified(self, change_id):
        """помечает запись об изменении расписания как обработанную"""
        return self._db.schedule_changes.update_one(
            filter={'_id': change_id}, update={'$set': {'notified': True}}
        )

    def get_tg_users_for_group(self, group: str) -> list:
        """возвращает telegram-пользователей, зарегистрированных на группу"""
        return list(self._db.users.find(filter={'group': group}))

    def save_status_tg(self, date, time):
        """сохраняем время последнего парса"""
        status = {
            'name': 'tg_reminders',
            'date': date,
            'time': time,
        }

        return self._db.status.update_one(filter={'name': 'tg_reminders'}, update={'$set': status}, upsert=True)

    def save_status_reminders_vk(self, date, time):
        """сохраняем время последнего парса"""
        status = {
            'name': 'vk_reminders',
            'date': date,
            'time': time,
        }

        return self._db.status.update_one(filter={'name': 'vk_reminders'}, update={'$set': status}, upsert=True)
