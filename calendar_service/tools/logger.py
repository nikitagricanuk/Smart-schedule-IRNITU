import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

format = '%(asctime)s [%(levelname)s] %(filename)s: %(message)s'
formatter = logging.Formatter(format, datefmt='%d.%m.%Y %H:%M:%S')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
