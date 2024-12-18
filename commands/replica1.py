#commands/replica1.py

import psycopg2
import click
import json
from utils.execute import execute_sql
from utils.config_loader import get_clusters_dict
from utils.repliaction_utils import create_schema, create_table, create_publication, create_subscription
from utils.log_handler import logger

def setup_replica1(ddl=False, logddl_tests=False):
    """Настройка первой реплики для логической репликации."""

    try:
        config = get_clusters_dict()
        replica1 = config['replica1']
        master = config['master']

        master_subscription_info = f'host=localhost port={master["port"]} dbname=postgres user=postgres'

        logger.debug("Настройка Replica 1...")

        logger.debug("Развертывание схемы и таблицы на Replica 1...")
        create_schema(conn_params=replica1['conn_params'], schema_name=replica1['replication_schema'], server_name=replica1['name'])
        create_table(conn_params=replica1['conn_params'], schema_name=replica1['replication_schema'], table_name=replica1['replication_table'], server_name=replica1['name'])

        logger.debug("Создание подписки на Master сервер на Replica1...")
        create_subscription(conn_params=replica1['conn_params'], subscription_name=f"sub_{replica1['name']}",connection_info=master_subscription_info, publication_name=f"pub_{master['name']}", server_name=replica1['name'])
        if(logddl_tests == False):
            logger.debug("Создание публикации на Replica 1...")
            create_publication(conn_params=replica1['conn_params'], publication_name=f"pub_{replica1['name']}", schema_name=replica1['replication_schema'], server_name=replica1['name'], ddl=ddl)
            logger.debug("Replica 1 настроена успешно.")

    except Exception as e:
        logger.error(f"Ошибка при настройке Replica 1: {e}")