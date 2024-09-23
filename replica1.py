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
        click.echo(f"Ошибка при выполнении SQL на Replica 1: {e}")

def setup_replica1():
    """Настройка Replica 1."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    replica1_conn_params = clusters['replica1']['conn_params']
    master_conn_params = clusters['master']['conn_params']
    replica1_publication = "replica1_publication"
    replica1_subscription = "replica1_subscription"
    master_publication = "master_publication"

    # Развертывание схемы и таблицы
    click.echo("Развертывание схемы и таблицы на Replica 1...")
    execute_sql(replica1_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    execute_sql(replica1_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(replica1_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)

    # Подписка на Master
    click.echo("Создание подписки на Master...")
    execute_sql(replica1_conn_params, f"DROP SUBSCRIPTION IF EXISTS {replica1_subscription};")
    execute_sql(replica1_conn_params, f"""
        CREATE SUBSCRIPTION {replica1_subscription}
        CONNECTION 'host=localhost port={clusters["master"]["port"]} dbname=postgres user=postgres'
        PUBLICATION {master_publication}
        WITH (copy_data = true);
    """)

    # Создание публикации
    click.echo("Создание публикации на Replica 1...")
    execute_sql(replica1_conn_params, f"DROP PUBLICATION IF EXISTS {replica1_publication};")
    execute_sql(replica1_conn_params, f"CREATE PUBLICATION {replica1_publication} FOR ALL TABLES IN SCHEMA {schema_name};")
