import logging
import os


def _configure_logging():
    log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    root_logger = logging.getLogger()
    if not any(getattr(handler, '_smart_schedule_handler', False) for handler in root_logger.handlers):
        root_logger.handlers.clear()
        console_handler = logging.StreamHandler()
        console_handler._smart_schedule_handler = True
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d %(message)s',
            datefmt='%d.%m.%Y %H:%M:%S',
        )
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    root_logger.setLevel(log_level)
    logging.getLogger('aiogram').setLevel(log_level)
    logging.captureWarnings(True)


_configure_logging()
logger = logging.getLogger('tg_bot')
