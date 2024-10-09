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
            subprocess.run(['make', 'clean'], cwd=pg_source_dir, check=True,  stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        logger.debug("Выполняется 'configure'...")
        subprocess.run(['./configure'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        logger.debug("Выполняется 'make'...")
        subprocess.run(['make'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, shell=True)

        logger.debug("Выполняется 'make install'...")
        subprocess.run(['make', 'install'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        logger.debug("PostgreSQL успешно собран и установлен....")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при выполнении команды: {e}")
        logger.debug("Подробности ошибки смотрите в 'build.log'")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        sys.exit(1)
