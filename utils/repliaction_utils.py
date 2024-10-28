# utils/replication_utils.py

import psycopg2
from utils.execute import execute_sql
from utils.config_loader import load_config
from utils.log_handler import logger
import click

def create_schema(conn_params, schema_name, server_name):
    """Создает схему в базе данных."""
    execute_sql(conn_params, f"CREATE SCHEMA IF NOT EXISTS {schema_name};", server_name=server_name)

def drop_schema(conn_params, schema_name, server_name):
    """Удаляет схему из базы данных."""
    execute_sql(conn_params, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;", server_name=server_name)

def create_table(conn_params, schema_name, table_name, server_name):
    """Создает таблицу в указанной схеме."""
    execute_sql(conn_params, f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    """, server_name=server_name)

def drop_table(conn_params, schema_name, table_name, server_name):
    """Удаляет таблицу из указанной схемы."""
    execute_sql(conn_params, f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;", server_name=server_name)


def format_option_value(value):
    """
    Форматирует значение опции для SQL-запроса.
    :param value: Значение опции.
    :return: Отформатированное значение в виде строки.
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, str):
        # Если значение содержит пробелы или специальные символы, обернем его в кавычки
        if ' ' in value or ',' in value or '=' in value:
            return f"'{value}'"
        else:
            return value
    else:
        return str(value)

def create_publication(conn_params, publication_name, schema_name, server_name, ddl = False):
    """Создает публикацию для указанной схемы."""
    drop_publication(conn_params, publication_name, server_name)
    try:
        # Создание публикации
        if ddl:
            create_publication_query = f"CREATE PUBLICATION {publication_name} FOR TABLES IN SCHEMA {schema_name} WITH (ddl = 'table');"
        else:
            create_publication_query = f"CREATE PUBLICATION {publication_name} FOR TABLES IN SCHEMA {schema_name};"

        execute_sql(conn_params,sql=create_publication_query, server_name=server_name)
    except psycopg2.errors.DuplicateObject:
        logger.warning(f"Publication '{publication_name}' already exists on server '{server_name}'. Skipping creation.")

def drop_publication(conn_params, publication_name, server_name):
    """Удаляет публикацию."""
    execute_sql(conn_params, f"DROP PUBLICATION IF EXISTS {publication_name};", server_name=server_name)

def create_subscription(conn_params, subscription_name, connection_info, publication_name, server_name, options=None):
    """
    Создает подписку на указанную публикацию с поддержкой дополнительных опций.

    :param conn_params: Параметры подключения к реплике.
    :param subscription_name: Имя создаваемой подписки.
    :param connection_info: Строка подключения к мастеру.
    :param publication_name: Имя публикации, на которую подписывается реплика.
    :param server_name: Имя сервера реплики для логирования.
    :param options: Словарь с опциями подписки (например, {'copy_data': 'false', 'synchronous_commit': 'local'}).
    """
    try:
        drop_subscription(conn_params, subscription_name, server_name)
        # Формируем строку опций для команды WITH, если опции заданы
        if options:
            options_str = ', '.join([f"{key} = {format_option_value(value)}" for key, value in options.items()])
            with_clause = f"WITH ({options_str})"
        else:
            with_clause = ""  # Без опций

        # Формирование SQL-запроса для создания подписки
        create_subscription_query = f"""
            CREATE SUBSCRIPTION {subscription_name}
            CONNECTION '{connection_info}'
            PUBLICATION {publication_name}
            {with_clause};
        """

        execute_sql(conn_params, create_subscription_query, server_name=server_name, autocommit=True)
        logger.debug(f"Подписка '{subscription_name}' успешно создана с опциями: {options_str if options else 'без опций'}")
    except Exception as e:
        print(f"Ошибка при создании подписки '{subscription_name}' на сервере '{server_name}': {e}")

def drop_subscription(conn_params, subscription_name, server_name):
    """Удаляет подписку."""
    execute_sql(conn_params, f"DROP SUBSCRIPTION IF EXISTS {subscription_name};", server_name=server_name, autocommit=True)
