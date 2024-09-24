#master.py
import psycopg2
import click
import json
from db_utils import execute_sql

def setup_master():
    """Настройка Master 1."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    server_name = clusters['master']['name']
    master_conn_params = clusters['master']['conn_params']
    master_publication = "master_publication"

    # Развертывание схемы и таблицы
    click.echo("Развертывание схемы и таблицы на Master...")
    execute_sql(master_conn_params, f"CREATE SCHEMA {schema_name};", server_name)
    execute_sql(master_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """, server_name)

    # Создание публикации
    click.echo("Создание публикации на Master...")
    execute_sql(master_conn_params, f"CREATE PUBLICATION {master_publication} FOR TABLES IN SCHEMA {schema_name};", server_name)
