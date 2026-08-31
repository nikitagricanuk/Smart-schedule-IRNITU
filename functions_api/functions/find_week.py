from datetime import datetime, timedelta
import pytz

TZ_IRKUTSK = pytz.timezone('Asia/Irkutsk')


def find_week():
    now = datetime.now(TZ_IRKUTSK)
    # При формате данных 01:01:23.283+00:00 (00 как на Гринвиче) ошибки с выводом расписания ночью не возникает
    # У нас формат данных 01:01:23.283+08:00
    # С 00.00.00 до 01.02.59 поломка
    error = False
    #now = datetime.fromisoformat(f'2021-02-28 01:03:00.000+08:00')
    if int(now.strftime('%H')) in [0, 1] and int(now.strftime('%M')) < 3:
        error = True
    sep = datetime(now.year if now.month >= 9 else now.year - 1, 9, 1, tzinfo=TZ_IRKUTSK)

    d1 = sep - timedelta(days=sep.weekday())
    d2 = now - timedelta(days=now.weekday())
    parity = ((d2 - d1).days // 7) % 2
    if error:
        return 'odd' if parity else 'even'

    return 'even' if parity else 'odd'