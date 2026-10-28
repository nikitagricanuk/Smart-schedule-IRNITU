"""Уведомляет пользователей Telegram об изменении расписания их группы и о разовых переносах пар."""

import time
from datetime import datetime, timedelta

from aiogram.utils.exceptions import BadRequest, Unauthorized

from tools.logger import logger
from tools.storage import MongodbService

POLL_INTERVAL_SEC = 60
# Сколько пытаемся доставить уведомление, прежде чем сдаться (старое уже неактуально).
MAX_DELIVERY_AGE = timedelta(hours=24)

# Ошибки, при которых повтор бесполезен: бот заблокирован, чат не найден и т.п.
PERMANENT_ERRORS = (Unauthorized, BadRequest)


WEEKDAYS = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']


def _format_day(iso_date: str) -> str:
    try:
        day = datetime.strptime(iso_date, '%Y-%m-%d')
    except (TypeError, ValueError):
        return iso_date or ''
    return f'{WEEKDAYS[day.weekday()]}, {day.strftime("%d.%m")}'


def _format_aud(aud) -> str:
    return ', '.join(item for item in aud or [] if item)


def transfer_text(group: str, transfer: dict) -> str:
    """Текст уведомления о разовом переносе: что было и что стало."""
    lines = [f'🔄 Перенос занятия, группа {group}',
             f'👉 {transfer.get("name")} {transfer.get("info") or ""}'.rstrip()]
    to_when = f'{_format_day(transfer.get("to_date"))} в {transfer.get("to_time")}'
    old_aud, new_aud = _format_aud(transfer.get('aud')), _format_aud(transfer.get('to_aud'))

    if not transfer.get('date'):
        # Исходную пару не нашли — сообщаем, где и когда пройдёт занятие.
        lines.append(f'📅 {to_when}')
        if new_aud:
            lines.append(f'🏫 Аудитория: {new_aud}')
    else:
        if transfer['date'] != transfer.get('to_date'):
            lines.append(f'📅 {_format_day(transfer["date"])} в {transfer.get("time")} → {to_when}')
        else:
            lines.append(f'📅 {_format_day(transfer["date"])}')
            if transfer.get('time') != transfer.get('to_time'):
                lines.append(f'🕒 Время: {transfer.get("time")} → {transfer.get("to_time")}')
            else:
                lines[-1] += f' в {transfer.get("time")}'
        if new_aud and new_aud != old_aud:
            lines.append(f'🏫 Аудитория: {old_aud} → {new_aud}' if old_aud else f'🏫 Аудитория: {new_aud}')
        elif new_aud:
            lines.append(f'🏫 Аудитория: {new_aud}')

    if transfer.get('reason'):
        lines.append(f'Причина: {transfer["reason"]}')
    return '\n'.join(lines)


def _user_in_subgroup(user: dict, subgroup) -> bool:
    """Перенос одной подгруппы не касается тех, кто выбрал в боте другую подгруппу."""
    user_subgroup = int(user.get('subgroup') or 0)
    return not subgroup or not user_subgroup or user_subgroup == subgroup


class DeliveryInterrupted(Exception):
    """Telegram недоступен: доставку нужно повторить на следующей итерации."""


def _parse_changed_at(value: str):
    try:
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except (TypeError, ValueError):
        return None


class ScheduleChangeNotifier:
    def __init__(self, bot, poll_interval_sec: int = POLL_INTERVAL_SEC, storage=None, now=datetime.utcnow):
        self.bot = bot
        self.poll_interval_sec = poll_interval_sec
        self.storage = storage if storage is not None else MongodbService().get_instance()
        self.now = now

    def _notify_group(self, change: dict):
        """Отправляет уведомление тем, кому оно ещё не доставлено.

        Каждая успешная (или безнадёжная) отправка сразу записывается в
        change['delivered_to'], поэтому при повторе никто не получит дубль.
        При сетевой ошибке бросает DeliveryInterrupted.
        """
        group = change['group']
        delivered = set(change.get('delivered_to') or [])
        transfer = change.get('transfer')
        if transfer:
            text = transfer_text(group, transfer)
        else:
            text = (
                f'📅 Расписание группы {group} изменилось!\n'
                f'Откройте раздел "Расписание", чтобы увидеть актуальные пары.'
            )
        for user in self.storage.get_tg_users_for_group(group):
            if transfer and not _user_in_subgroup(user, transfer.get('subgroup')):
                continue
            chat_id = user.get('chat_id')
            if not chat_id or chat_id in delivered:
                continue
            try:
                self.bot.send_message(chat_id=chat_id, text=text)
            except PERMANENT_ERRORS as exc:
                logger.warning(f'Schedule change notification to {chat_id} rejected by Telegram, skipping: {exc}')
            except Exception as exc:
                logger.exception(f'Failed to send schedule change notification to {chat_id}: {exc}')
                raise DeliveryInterrupted(str(exc)) from exc
            self.storage.add_schedule_change_delivery(change['_id'], chat_id)
            delivered.add(chat_id)

    def process_pending(self):
        for change in self.storage.get_pending_schedule_changes():
            changed_at = _parse_changed_at(change.get('changed_at'))
            if changed_at and self.now() - changed_at > MAX_DELIVERY_AGE:
                logger.error(f'Giving up on schedule change notification for {change["group"]} '
                             f'from {change["changed_at"]}: not delivered within {MAX_DELIVERY_AGE}')
                self.storage.mark_schedule_change_notified(change['_id'], gave_up=True)
                continue
            try:
                self._notify_group(change)
            except DeliveryInterrupted:
                # Telegram недоступен: остальные записи остаются pending до следующей итерации.
                return
            self.storage.mark_schedule_change_notified(change['_id'])

    def run(self):
        logger.info('schedule_change_notifier started')
        while True:
            try:
                self.process_pending()
            except Exception as exc:
                logger.exception(f'schedule_change_notifier iteration failed: {exc}')
            time.sleep(self.poll_interval_sec)
