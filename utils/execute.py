# utils/execute.py
import json
import psycopg2
import click
import subprocess
import sys
from utils.log_handler import logger

def execute_sql(conn_params, sql, server_name, autocommit=False, fetch=False):
    """Выполнение SQL-команды с указанием имени сервера в случае ошибки."""
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = autocommit

        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch:
                results = cur.fetchall()
            else:
                results = None

        if not autocommit:
            conn.commit()

        conn.close()
        return results
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL на сервере {server_name}: {e}")
        return None

def run_as_postgres(command):
    """Запуск команды от имени пользователя postgres"""
    try:
        subprocess.run(['sudo', '-u', 'postgres', 'bash', '-c', ' '.join(command)], check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Ошибка при выполнении команды от имени postgres: {e}")
        logger.error(str(e))
        sys.exit(1)
