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
        click.echo(f"Ошибка при выполнении SQL на Replica 2: {e}")

def setup_replica2():
    """Настройка Replica 2."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    replica2_conn_params = clusters['replica2']['conn_params']
    replica1_conn_params = clusters['replica1']['conn_params']
    replica2_subscription = "replica2_subscription"
    replica1_publication = "replica1_publication"

    # Развертывание схемы и таблицы
    click.echo("Развертывание схемы и таблицы на Replica 2...")
    execute_sql(replica2_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    execute_sql(replica2_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(replica2_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)

    # Подписка на Replica 1
    click.echo("Создание подписки на Replica 1...")
    execute_sql(replica2_conn_params, f"DROP SUBSCRIPTION IF EXISTS {replica2_subscription};")
    execute_sql(replica2_conn_params, f"""
        CREATE SUBSCRIPTION {replica2_subscription}
        CONNECTION 'host=localhost port={clusters["replica1"]["port"]} dbname=postgres user=postgres'
        PUBLICATION {replica1_publication}
        WITH (copy_data = true);
    """)
