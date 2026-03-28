import asyncio
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Dict, Optional, Tuple


@dataclass
class PendingStep:
    callback: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class StepHandlerRegistry:
    def __init__(self):
        self._steps: Dict[int, PendingStep] = {}
        self._lock = RLock()

    def register(self, chat_id: int, callback: Callable[..., Any], *args, **kwargs):
        with self._lock:
            self._steps[chat_id] = PendingStep(callback=callback, args=args, kwargs=kwargs)

    def pop(self, chat_id: int) -> Optional[PendingStep]:
        with self._lock:
            return self._steps.pop(chat_id, None)

    def clear(self, chat_id: int):
        with self._lock:
            self._steps.pop(chat_id, None)


class AiogramTeleBotCompat:
    """Минимальный sync-совместимый слой над aiogram Bot для старой бизнес-логики."""

    def __init__(self, bot, step_registry: StepHandlerRegistry):
        self._bot = bot
        self._step_registry = step_registry
        self._loop = None

    def attach_loop(self, loop):
        self._loop = loop

    def _run(self, coro):
        if self._loop is None:
            raise RuntimeError('Aiogram loop is not attached yet')
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    @staticmethod
    def _get_arg(args, kwargs, name, position):
        if name in kwargs:
            return kwargs.pop(name)
        if len(args) > position:
            return args[position]
        return None

    def send_message(self, *args, **kwargs):
        chat_id = self._get_arg(args, kwargs, 'chat_id', 0)
        text = self._get_arg(args, kwargs, 'text', 1)
        return self._run(self._bot.send_message(chat_id=chat_id, text=text, **kwargs))

    def send_photo(self, *args, **kwargs):
        chat_id = self._get_arg(args, kwargs, 'chat_id', 0)
        photo = self._get_arg(args, kwargs, 'photo', 1)
        return self._run(self._bot.send_photo(chat_id=chat_id, photo=photo, **kwargs))

    def edit_message_text(self, *args, **kwargs):
        chat_id = self._get_arg(args, kwargs, 'chat_id', 0)
        message_id = self._get_arg(args, kwargs, 'message_id', 1)
        text = self._get_arg(args, kwargs, 'text', 2)
        return self._run(
            self._bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, **kwargs)
        )

    def edit_message_reply_markup(self, *args, **kwargs):
        chat_id = self._get_arg(args, kwargs, 'chat_id', 0)
        message_id = self._get_arg(args, kwargs, 'message_id', 1)
        return self._run(
            self._bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, **kwargs)
        )

    def delete_message(self, *args, **kwargs):
        chat_id = self._get_arg(args, kwargs, 'chat_id', 0)
        message_id = self._get_arg(args, kwargs, 'message_id', 1)
        return self._run(self._bot.delete_message(chat_id=chat_id, message_id=message_id))

    def answer_callback_query(self, *args, **kwargs):
        callback_query_id = self._get_arg(args, kwargs, 'callback_query_id', 0)
        return self._run(self._bot.answer_callback_query(callback_query_id=callback_query_id, **kwargs))

    def remove_webhook(self):
        return self._run(self._bot.delete_webhook())

    def register_next_step_handler(self, message, callback: Callable[..., Any], *args, **kwargs):
        self._step_registry.register(message.chat.id, callback, *args, **kwargs)

    def clear_step_handler_by_chat_id(self, chat_id: int):
        self._step_registry.clear(chat_id)
