#commands/master.py

import psycopg2
import click
import json
from utils.execute import execute_sql
from utils.config_loader import get_clusters_dict
from utils.repliaction_utils import create_schema, create_table, create_publication
from utils.log_handler import logger

def setup_master(ddl=False):
    """Настройка мастера для логической репликации."""
    try:
        clusters = get_clusters_dict()
        master = clusters['master']

        # Развертывание схемы и таблицы
        logger.debug(f"Развертывание схемы и таблицы на {master['name']}...")
        create_schema(conn_params=master['conn_params'], schema_name= master['replication_schema'], server_name=master['name'])
        create_table(conn_params=master['conn_params'], schema_name= master['replication_schema'], table_name= master['replication_table'], server_name=master['name'])

        logger.debug("Создание публикации на Master...")
        create_publication(conn_params=master['conn_params'], publication_name=f"pub_{master['name']}", schema_name= master['replication_schema'], server_name=master['name'], ddl=ddl)
        logger.debug("Мастер настроен успешно.")
    except Exception as e:
        logger.error(f"Ошибка при настройке мастера: {e}")
