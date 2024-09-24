#replica1.py
import psycopg2
import click
import json
from db_utils import execute_sql

def setup_replica1():
    """Настройка Replica 1."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    server_name = clusters['replica1']['name']
    replica1_conn_params = clusters['replica1']['conn_params']
    master_conn_params = clusters['master']['conn_params']
    replica1_publication = "replica1_publication"
    replica1_subscription = "replica1_subscription"
    master_publication = "master_publication"

    # Развертывание схемы и таблицы
    click.echo("Развертывание схемы и таблицы на Replica 1...")
    execute_sql(replica1_conn_params, f"CREATE SCHEMA IF NOT EXISTS {schema_name};", server_name)
    execute_sql(replica1_conn_params, f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """, server_name)

    # Подписка на Master
    click.echo("Создание подписки на Master...")
    execute_sql(replica1_conn_params, f"""
        CREATE SUBSCRIPTION {replica1_subscription}
        CONNECTION 'host=localhost port={clusters["master"]["port"]} dbname=postgres user=postgres'
        PUBLICATION {master_publication}
        WITH (copy_data = true);
    """, server_name, autocommit=True)

    # Создание публикации
    click.echo("Создание публикации на Replica 1...")
    execute_sql(replica1_conn_params, f"CREATE PUBLICATION {replica1_publication} FOR TABLES IN SCHEMA {schema_name};", server_name)
