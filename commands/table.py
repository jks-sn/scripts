# commands/table.py

from utils.execute import execute_sql

def get_table_columns(conn_params, server_name, schema_name, table_name):
    """Получить список колонок таблицы."""
    query = f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
    """
    return execute_sql(conn_params, query, server_name fetch=True)

def check_table_exists(conn_params, server_name, schema_name, table_name):
    """Проверить, существует ли таблица."""
    query = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        );
    """
    result = execute_sql(conn_params, query, fetch=True)
    return result[0][0]
