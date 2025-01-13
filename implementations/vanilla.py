# implementations/vanilla.py

from interfaces.ddl_interface import DDLInterface
from utils.execute import execute_sql
from utils.log_handler import logger
from utils.sql_templates import (
    generate_create_publication_query,
    generate_create_schema_query,
    generate_create_subscription_query,
    generate_create_table_query,
    generate_drop_publication_query,
    generate_drop_schema_query,
    generate_drop_subscription_query,
    generate_drop_table_query
)

class Vanilla(DDLInterface):
    def __init__(self, config):
        self.config = config
        self.cluster_conn = {}
        for cluster in config.clusters:
            self.cluster_conn[cluster.name] = cluster.conn_params.model_dump()

    def _execute(self, cluster_name: str, sql: str, autocommit: bool = False):
        conn_params = self.cluster_conn.get(cluster_name)
        if not conn_params:
            raise ValueError(f"Unknown cluster name: {cluster_name}")
        execute_sql(conn_params, sql, server_name=cluster_name, autocommit=autocommit)

    def create_publication(self, cluster_name: str, publication_name: str, schema_name: str, ddl: bool):
        sql = generate_create_publication_query(publication_name, schema_name, ddl=False)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Publication '{publication_name}' created on '{cluster_name}'")

    def drop_publication(self, cluster_name: str, publication_name: str):
        sql = generate_drop_publication_query(publication_name)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Publication '{publication_name}' dropped on '{cluster_name}'")

    def create_subscription(self, cluster_name: str, subscription_name: str, connection_info: str, publication_name: str):
        sql = generate_create_subscription_query(subscription_name, connection_info, publication_name)
        self._execute(cluster_name, sql, autocommit=True)
        logger.debug(f"[Vanilla] Subscription '{subscription_name}' created on '{cluster_name}'")

    def drop_subscription(self, cluster_name: str, subscription_name: str):
        sql = generate_drop_subscription_query(subscription_name)
        self._execute(cluster_name, sql, autocommit=True)
        logger.debug(f"[Vanilla] Subscription '{subscription_name}' dropped on '{cluster_name}'")

    def create_schema(self, cluster_name: str, schema_name: str):
        sql = generate_create_schema_query(schema_name)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Schema '{schema_name}' created on '{cluster_name}'")

    def drop_schema(self, cluster_name: str, schema_name: str):
        sql = generate_drop_schema_query(schema_name)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Schema '{schema_name}' dropped on '{cluster_name}'")

    def create_table(self, cluster_name: str, schema_name: str, table_name: str):
        sql = generate_create_table_query(schema_name, table_name)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Table '{schema_name}.{table_name}' created on '{cluster_name}'")

    def drop_table(self, cluster_name: str, schema_name: str, table_name: str):
        sql = generate_drop_table_query(schema_name, table_name)
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Table '{schema_name}.{table_name}' dropped on '{cluster_name}'")

    def add_column(self, cluster_name: str, schema_name: str, table_name: str,
                   column_name: str, column_type: str = "INTEGER", default_value=None):
        default_clause = f"DEFAULT {default_value}" if default_value is not None else ""
        sql = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} {column_type} {default_clause};"
        self._execute(cluster_name, sql)
        logger.debug(f"[Vanilla] Added column {column_name} to {schema_name}.{table_name} on '{cluster_name}'")

    def table_exists(self, cluster_name: str, schema_name: str, table_name: str) -> bool:
        sql = f"""
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}'
        );
        """
        conn_params = self.cluster_conn.get(cluster_name)
        results = execute_sql(conn_params, sql, server_name=cluster_name, fetch=True)
        return results and results[0][0] is True

    def get_table_columns(self, cluster_name: str, schema_name: str, table_name: str):
        sql = f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
          AND table_name = '{table_name}';
        """
        conn_params = self.cluster_conn.get(cluster_name)
        results = execute_sql(conn_params, sql, server_name=cluster_name, fetch=True)
        columns = []
        for row in results:
            col_name, col_type, col_default = row
            columns.append({
                "column_name": col_name,
                "data_type": col_type,
                "default": col_default
            })
        return columns
