#commands/replica2.py

import psycopg2
import click
import json
from utils.execute import execute_sql
from utils.config_loader import get_clusters_dict
from utils.repliaction_utils import create_schema, create_table, create_publication, create_subscription
from utils.log_handler import logger
def setup_replica2():
    """Настройка второй реплики для логической репликации."""

    try:
        config = get_clusters_dict()

        replica1 = config['replica1']
        replica2 = config['replica2']

        replica1_subscription_info = f'host=localhost port={replica1["port"]} dbname=postgres user=postgres'
        # Развертывание схемы и таблицы
        logger.debug("Развертывание схемы и таблицы на Replica 2...")
        create_schema(conn_params=replica2['conn_params'], schema_name=replica2['replication_schema'], server_name=replica2['name'])
        create_table(conn_params=replica2['conn_params'], schema_name=replica2['replication_schema'], table_name=replica2['replication_table'], server_name=replica2['name'])

        # Подписка на Replica 1
        logger.debug("Создание подписки на Replica 1...")
        create_subscription(conn_params=replica2['conn_params'], subscription_name=f"sub_{replica2['name']}", connection_info=replica1_subscription_info, publication_name=f"pub_{replica1['name']}", server_name=replica2['name'])

        logger.debug("Replica 2 настроена успешно.")

    except Exception as e:
        logger.error(f"Ошибка при настройке Replica 2: {e}")
