#tests.py
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

def run_tests():
    """Выполнение тестовых сценариев."""
    with open('config.json') as f:
        config = json.load(f)

    schema_name = config['schema_name']
    table_name = config['table_name']
    clusters = {cluster['name']: cluster for cluster in config['clusters']}

    master_conn_params = clusters['master']['conn_params']
    replica2_conn_params = clusters['replica2']['conn_params']

    # Вставка данных на Master
    click.echo("Вставка данных на Master...")
    execute_sql(master_conn_params, f"""
        INSERT INTO {schema_name}.{table_name} (data)
        VALUES ('Test data from master');
    """)

    # Задержка для репликации
    time.sleep(5)

    # Проверка данных на Replica 2
    click.echo("Проверка данных на Replica 2...")
    results = execute_sql(replica2_conn_params, f"SELECT * FROM {schema_name}.{table_name};", fetch=True)

    if results:
        click.echo("Данные на Replica 2:")
        for row in results:
            click.echo(row)
    else:
        click.echo("Данные не найдены на Replica 2.")
