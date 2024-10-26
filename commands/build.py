#commands/build.py

import subprocess
import sys
import os
import click
import json
from utils.config_loader import load_config
from utils.log_handler import setup_logger
from utils.log_handler import logger

def build_postgresql(clean):
    """Функция для сборки и установки PostgreSQL."""
    try:
        config = load_config()
        pg_source_dir = config['pg_source_dir']

        if not os.path.exists(pg_source_dir):
            click.echo(f"Путь к исходникам PostgreSQL не найден: {pg_source_dir}")
            sys.exit(1)

        if clean:
            logger.debug("Очистка предыдущей сборки...")
            result = subprocess.run(['make', 'clean'], cwd=pg_source_dir, check=True,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            logger.debug(f"make clean output:\n{result.stdout}")

        logger.debug("Выполняется 'configure'...")
        result = subprocess.run(['./configure'], cwd=pg_source_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        logger.debug(f"./configure output:\n{result.stdout}")

        logger.debug("Выполняется 'make'...")
        result = subprocess.run(['make'], cwd=pg_source_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, text=True)
        logger.debug(f"make output:\n{result.stdout}")

        logger.debug("Выполняется 'make install'...")
        result = subprocess.run(['make', 'install'], cwd=pg_source_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        logger.debug(f"make install output:\n{result.stdout}")

        logger.debug("PostgreSQL успешно собран и установлен....")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при выполнении команды: {e.cmd}")
        logger.error(f"Возвратный код: {e.returncode}")
        logger.error(f"Вывод команды:\n{e.output}")
        logger.debug("Подробности ошибки смотрите в лог-файле.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        sys.exit(1)
