from datetime import date, datetime, timedelta
import pytz

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')


def find_week():
    """Чётность текущей учебной недели ('odd' / 'even').

    ИРНИТУ ведёт непрерывный счёт недель от той, на которую приходится 1 сентября
    (она считается ЧЁТНОЙ — на сайте это класс-контейнер ``sch-list-week-even``).
    Каждая следующая календарная неделя меняет чётность.

    Раньше здесь возвращалась противоположная чётность, из-за чего расписание
    показывало пары «серой» (следующей) недели как пары текущей. Ночной костыль
    с ``error`` тоже убран: ``datetime.now(TZ_IRKUTSK)`` уже привязан к иркутскому
    времени, поэтому неделя переключается в понедельник 00:00 по Иркутску.
    """
    today = datetime.now(TZ_IRKUTSK).date()
    sep = date(today.year if today.month >= 9 else today.year - 1, 9, 1)

    week0_monday = sep - timedelta(days=sep.weekday())
    current_monday = today - timedelta(days=today.weekday())
    parity = ((current_monday - week0_monday).days // 7) % 2

    return 'odd' if parity else 'even'
