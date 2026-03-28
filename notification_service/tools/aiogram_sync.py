import asyncio
from threading import Event, Thread

from aiogram import Bot, types


class SyncAiogramBot:
    """Потокобезопасная sync-обертка над aiogram Bot для фоновых сервисов."""

    def __init__(self, token: str):
        token = (token or '').strip()
        if not token:
            raise RuntimeError('Environment variable TG_TOKEN is not set')
        self._token = token
        self._bot = None
        self._loop = asyncio.new_event_loop()
        self._ready = Event()
        self._thread = Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._bot = Bot(token=self._token)
        self._ready.set()
        self._loop.run_forever()

    def _submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    @staticmethod
    def _prepare_file(file_obj):
        if isinstance(file_obj, (str, types.InputFile)):
            return file_obj
        return types.InputFile(file_obj)

    def send_message(self, chat_id, text, **kwargs):
        return self._submit(self._bot.send_message(chat_id=chat_id, text=text, **kwargs))

    def send_photo(self, chat_id, photo, **kwargs):
        photo = self._prepare_file(photo)
        return self._submit(self._bot.send_photo(chat_id=chat_id, photo=photo, **kwargs))

    def close(self):
        if self._bot is None:
            return
        self._submit(self._bot.session.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
