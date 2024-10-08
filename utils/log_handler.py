#utils/log_handler.py

import os
import logging
import time

def get_log_file(log_filename):
    """Получает путь к файлу логов и создает папку, если она не существует."""

    logs_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = os.path.join(logs_dir, log_filename)
    return log_file_path

def setup_logger():
    """Настраивает логгер для записи в файл."""
    log_filename = time.strftime("log_%Y%m%d_%H%M%S.log")

    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger


logger = setup_logger()