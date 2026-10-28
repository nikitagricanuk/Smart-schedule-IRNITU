"""Что изменилось в расписании групп — для уведомлений пользователей.

Изменения делятся на два вида:
  * регулярное расписание (пары по дням недели) — общее уведомление
    «расписание изменилось»;
  * новые разовые переносы — отдельное уведомление по каждому переносу с тем,
    что именно изменилось (аудитория, время, дата).
Разовые занятия и переносы не влияют на первое: иначе каждый новый или
прошедший перенос слал бы ещё и общее уведомление.
"""
import hashlib
import json
from datetime import date
from typing import Any, Dict, List

from functions.transfers import lesson_subgroup


def _is_one_off(lesson: Dict[str, Any]) -> bool:
    # Старые записи (до разбора переносов) узнаём по названию.
    return bool(lesson.get('date')) or (lesson.get('name') or '').lower().startswith('разовый перенос')


def regular_schedule(schedule: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Расписание без разовых занятий и переносов."""
    result = []
    for day in schedule or []:
        lessons = [
            {key: value for key, value in lesson.items() if key != 'transfers'}
            for lesson in day.get('lessons') or []
            if not _is_one_off(lesson)
        ]
        result.append({'day': day.get('day'), 'lessons': lessons})
    return result


def _hash(data) -> str:
    data_json = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(data_json.encode('utf-8')).hexdigest()


def _has_parsed_transfers(docs: List[Dict[str, Any]]) -> bool:
    for doc in docs:
        for day in doc.get('schedule') or []:
            for lesson in day.get('lessons') or []:
                if lesson.get('date') or lesson.get('transfers'):
                    return True
    return False


def detect_changed_groups(old_docs: List[Dict[str, Any]], new_docs: List[Dict[str, Any]]) -> List[str]:
    """Группы, у которых изменилось регулярное расписание.

    При первом наполнении базы (old_docs пуст) уведомлять не о чем — это не
    изменение, а первичная загрузка данных.
    """
    if not old_docs:
        return []

    old_hashes = {doc['group']: _hash(regular_schedule(doc.get('schedule', []))) for doc in old_docs if doc.get('group')}
    changed_groups = []
    for doc in new_docs:
        group = doc.get('group')
        if not group or not doc.get('schedule'):
            continue
        old_hash = old_hashes.get(group)
        if old_hash is None:
            continue  # новая группа - не с чем сравнивать, уведомлять некого
        if old_hash != _hash(regular_schedule(doc['schedule'])):
            changed_groups.append(group)

    return changed_groups


def group_transfers(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Все переносы группы: {ключ: описание для уведомления}."""
    result = {}
    for day in doc.get('schedule') or []:
        for lesson in day.get('lessons') or []:
            base = {
                'name': lesson.get('name'),
                'info': lesson.get('info'),
                'subgroup': lesson_subgroup(lesson.get('info')),
            }
            for record in lesson.get('transfers') or []:
                item = dict(base, time=lesson.get('time'), aud=lesson.get('aud'), **record)
                result[_hash(['transfer', item])] = item
            if lesson.get('date') and lesson.get('transfer'):
                # Перенос, для которого не нашлась исходная пара.
                item = dict(base, to_date=lesson['date'], to_time=lesson.get('time'), to_aud=lesson.get('aud'))
                result[_hash(['one-off', item])] = item
    return result


def detect_new_transfers(
    old_docs: List[Dict[str, Any]],
    new_docs: List[Dict[str, Any]],
    today: date,
) -> List[Dict[str, Any]]:
    """Новые (ещё не наступившие) переносы: [{'group': ..., 'transfer': {...}}].

    Если в старых данных переносы ещё не разбирались (первый запуск после
    обновления), не уведомляем: все существующие переносы оказались бы «новыми».
    """
    if not old_docs or not _has_parsed_transfers(old_docs):
        return []

    old_by_group = {doc['group']: doc for doc in old_docs if doc.get('group')}
    today_iso = today.isoformat()
    result = []
    for doc in new_docs:
        group = doc.get('group')
        old_doc = old_by_group.get(group)
        if not group or old_doc is None:
            continue
        old_keys = set(group_transfers(old_doc))
        for key, item in group_transfers(doc).items():
            if key in old_keys:
                continue
            if max(item.get('date') or '', item.get('to_date') or '') < today_iso:
                continue
            result.append({'group': group, 'transfer': item})
    return result

