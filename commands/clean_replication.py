#commands/clean_replication.py

import json
import click
from utils.execute import execute_sql
from utils.config_loader import load_config
from utils.log_handler import logger

def clean_replication():
    """Очистка подписок, публикаций и схем на всех серверах."""
    try:
        config = load_config()
        clusters = config['clusters']

        for cluster in clusters:
            server_name = cluster['name']
            conn_params = cluster['conn_params']
            schema_name = cluster['replication_schema'] if 'replication_schema' in cluster else 'replication'

            logger.debug(f"Очистка на сервере {server_name}...")

            execute_sql(conn_params=conn_params, sql=f"DROP SUBSCRIPTION IF EXISTS {server_name}_subscription;", server_name=server_name, autocommit=True)
            execute_sql(conn_params=conn_params, sql=f"DROP PUBLICATION {server_name}_publication;", server_name=server_name)
            execute_sql(conn_params=conn_params, sql=f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;", server_name=server_name)

        logger.debug("Очистка репликации завершена для всех кластеров.")
    except Exception as e:
        logger.error(f"Ошибка при очистке репликации: {e}")