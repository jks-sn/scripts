#commands/replication.py

import click
from commands.master import setup_master
from commands.replica1 import setup_replica1
from commands.replica2 import setup_replica2
from utils.log_handler import logger

def setup_replication(ddl=False):
    """Настройка логической репликации между мастером и репликами."""
    try:
        logger.debug("Начинается настройка мастера...")
        setup_master(ddl=ddl)
        logger.debug("Настройка мастера завершена.")
    except Exception as e:
        logger.error(f"Ошибка при настройке мастера: {e}")
        return

    try:
        logger.debug("Начинается настройка Replica 1...")
        setup_replica1(ddl=ddl)
        logger.debug("Настройка Replica 1 завершена.")
    except Exception as e:
        logger.error(f"Ошибка при настройке Replica 1: {e}")
        return

    try:
        logger.debug("Начинается настройка Replica 2...")
        setup_replica2()
        logger.debug("Настройка Replica 2 завершена.")
    except Exception as e:
        logger.error(f"Ошибка при настройке Replica 2: {e}")
        return

    logger.debug("Настройка репликации завершена.")

