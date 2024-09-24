# db_utils.py
import json
import psycopg2
import click

def execute_sql(conn_params, sql, server_name, autocommit=False):
    """Выполнение SQL-команды с указанием имени сервера в случае ошибки."""
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = autocommit

        # Use 'with' for the cursor, which is safe
        with conn.cursor() as cur:
            cur.execute(sql)

        # Если autocommit выключен, явно коммитим транзакцию
        if not autocommit:
            conn.commit()
            
        # Явно закрываем соединение 
        conn.close()
    except Exception as e:
        click.echo(f"Ошибка при выполнении SQL на сервере {server_name}: {e}")

def clean_replication_setup():
    """Очистка подписок, публикаций и схем на всех серверах."""
    
    with open('config.json') as f:
        config = json.load(f)

    clusters = config['clusters']
    for cluster in clusters:
        server_name = cluster['name']
        conn_params = cluster['conn_params']
        schema_name = cluster['schema_name'] if 'schema_name' in cluster else 'replication'
        
        click.echo(f"Очистка на сервере {server_name}...")
        
        # Удаление подписок и публикаций
        execute_sql(conn_params, f"DROP SUBSCRIPTION IF EXISTS {server_name}_subscription;", server_name, autocommit=True)
        execute_sql(conn_params, f"DROP PUBLICATION {server_name}_publication;", server_name)
        
        # Удаление схемы
        execute_sql(conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;", server_name)
