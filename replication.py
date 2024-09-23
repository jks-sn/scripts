import psycopg2
import click
import json
import time

def execute_sql(conn_params, sql, fetch=False):
    """Выполнение SQL-команды."""
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                if fetch:
                    return cur.fetchall()
    except Exception as e:
        click.echo(f"Ошибка при выполнении SQL: {e}")

def setup_replication():
    """Настройка репликации между кластерами."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    # Параметры подключения
    master_conn_params = clusters['master']['conn_params']
    replica1_conn_params = clusters['replica1']['conn_params']
    replica2_conn_params = clusters['replica2']['conn_params']

    # Имена публикаций и подписок
    master_publication = "master_publication"
    replica1_publication = "replica1_publication"
    replica1_subscription = "replica1_subscription"
    replica2_subscription = "replica2_subscription"

    # Шаги настройки репликации

    # Шаг 3: Удаление подписки на replica2
    click.echo("Удаление подписки на replica2...")
    execute_sql(replica2_conn_params, f"DROP SUBSCRIPTION IF EXISTS {replica2_subscription};")

    # Шаг 4: Удаление схемы на replica2
    click.echo("Удаление схемы на replica2...")
    execute_sql(replica2_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")

    # Шаг 5: Создание схемы и таблицы на replica2
    click.echo("Создание схемы и таблицы на replica2...")
    execute_sql(replica2_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(replica2_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)

    # Шаг 6: Удаление публикации на replica1
    click.echo("Удаление публикации на replica1...")
    execute_sql(replica1_conn_params, f"DROP PUBLICATION IF EXISTS {replica1_publication};")

    # Шаг 7: Удаление подписки на replica1
    click.echo("Удаление подписки на replica1...")
    execute_sql(replica1_conn_params, f"DROP SUBSCRIPTION IF EXISTS {replica1_subscription};")

    # Шаги 8-9: Повторение шагов на replica1 и создание публикации
    click.echo("Удаление схемы на replica1...")
    execute_sql(replica1_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    click.echo("Создание схемы и таблицы на replica1...")
    execute_sql(replica1_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(replica1_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)
    click.echo("Создание публикации на replica1...")
    execute_sql(replica1_conn_params, f"CREATE PUBLICATION {replica1_publication} FOR ALL TABLES IN SCHEMA {schema_name};")

    # Шаг 10: Повторение шагов на master
    click.echo("Удаление публикации на master...")
    execute_sql(master_conn_params, f"DROP PUBLICATION IF EXISTS {master_publication};")
    click.echo("Удаление схемы на master...")
    execute_sql(master_conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    click.echo("Создание схемы и таблицы на master...")
    execute_sql(master_conn_params, f"CREATE SCHEMA {schema_name};")
    execute_sql(master_conn_params, f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """)
    click.echo("Создание публикации на master...")
    execute_sql(master_conn_params, f"CREATE PUBLICATION {master_publication} FOR ALL TABLES IN SCHEMA {schema_name};")

    # Шаг 11: Создание подписки на replica2
    click.echo("Создание подписки на replica2...")
    execute_sql(replica2_conn_params, f"""
        CREATE SUBSCRIPTION {replica2_subscription}
        CONNECTION 'host=localhost port={clusters["replica1"]["port"]} dbname=postgres user=postgres'
        PUBLICATION {replica1_publication}
        WITH (copy_data = true);
    """)

    # Шаг 12: Создание подписки на replica1
    click.echo("Создание подписки на replica1...")
    execute_sql(replica1_conn_params, f"""
        CREATE SUBSCRIPTION {replica1_subscription}
        CONNECTION 'host=localhost port={clusters["master"]["port"]} dbname=postgres user=postgres'
        PUBLICATION {master_publication}
        WITH (copy_data = true);
    """)

    # Небольшая задержка для установки подписок
    time.sleep(5)
