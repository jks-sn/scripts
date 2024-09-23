import psycopg2
import click
import json

def execute_sql(conn_params, sql):
    """Выполнение SQL-команды."""
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
    except Exception as e:
        click.echo(f"Ошибка при выполнении SQL на Master: {e}")

def setup_master():
    """Настройка Master 1."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    master_conn_params = clusters['master']['conn_params']
    master_publication = "master_publication"

    # Развертывание схемы и таблицы
    click.echo("Развертывание схемы и таблицы на Master...")
    execute_sql(master_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    execute_sql(master_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(master_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)

    # Создание публикации
    click.echo("Создание публикации на Master...")
    execute_sql(master_conn_params, f"DROP PUBLICATION IF EXISTS {master_publication};")
    execute_sql(master_conn_params, f"CREATE PUBLICATION {master_publication} FOR ALL TABLES IN SCHEMA {schema_name};")
