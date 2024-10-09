# utils/replication_utils.py

from utils.execute import execute_sql

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


def create_publication(conn_params, publication_name, schema_name, server_name, ddl = False):
    """Создает публикацию для указанной схемы."""
    # Создание публикации
    if ddl:
        create_publication_query = f"CREATE PUBLICATION {publication_name} FOR TABLES IN SCHEMA {schema_name} WITH (ddl = 'table');"
    else:
        create_publication_query = f"CREATE PUBLICATION {publication_name} FOR TABLES IN SCHEMA {schema_name};"

    execute_sql(conn_params,sql=create_publication_query, server_name=server_name)

def drop_publication(conn_params, publication_name, server_name):
    """Удаляет публикацию."""
    execute_sql(conn_params, f"DROP PUBLICATION{publication_name};", server_name=server_name)

def create_subscription(conn_params, subscription_name, connection_info, publication_name, server_name):
    """Создает подписку на указанную публикацию."""
    execute_sql(conn_params, f"""
        CREATE SUBSCRIPTION {subscription_name}
        CONNECTION '{connection_info}'
        PUBLICATION {publication_name}
        WITH (copy_data = true);
    """, server_name=server_name, autocommit=True)

def drop_subscription(conn_params, subscription_name, server_name):
    """Удаляет подписку."""
    execute_sql(conn_params, f"DROP SUBSCRIPTION IF EXISTS {subscription_name};", server_name=server_name, autocommit=True)
