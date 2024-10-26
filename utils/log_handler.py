#utils/log_handler.py

import os
import logging
import time

def get_log_file():
    """Получает путь к файлу логов и создает папку, если она не существует."""
    date_str = time.strftime("%Y_%m_%d")

    logs_dir = os.path.join(os.path.dirname(__file__), '..', 'logs', date_str)
    os.makedirs(logs_dir, exist_ok=True)

    log_filename = time.strftime("log_%H%M%S.log")
    log_file_path = os.path.join(logs_dir, log_filename)

    return log_file_path

def setup_logger(log_filename=None):
    """Настраивает логгер для записи в файл."""
    log_file_path = get_log_file()

    logger = logging.getLogger("postgresql_build_logger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger

logger = setup_logger()