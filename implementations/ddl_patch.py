# implementations/ddl_patch.py

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
    generate_drop_table_query)

class DDLPatch(DDLInterface):
    def __init__(self, conn_params: dict, server_name: str):
        self.conn_params = conn_params
        self.server_name = server_name

    def create_schema(self, schema_name: str):
        sql = generate_create_schema_query(schema_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Schema '{schema_name}' created on {self.server_name}")

    def drop_schema(self, schema_name: str):
        sql = generate_drop_schema_query(schema_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Schema '{schema_name}' dropped on {self.server_name}")

    def create_table(self, schema_name: str, table_name: str):
        sql = generate_create_table_query(schema_name, table_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Table '{schema_name}.{table_name}' created on {self.server_name}")

    def drop_table(self, schema_name: str, table_name: str):
        sql = generate_drop_table_query(schema_name, table_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Table '{schema_name}.{table_name}' dropped on {self.server_name}")

    def create_publication(self, publication_name: str, schema_name: str, ddl: bool):
        sql = generate_create_publication_query(publication_name, schema_name, ddl)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Publication '{publication_name}' created on {self.server_name} with ddl={ddl}")

    def drop_publication(self, publication_name: str):
        sql = generate_drop_publication_query(publication_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Publication '{publication_name}' dropped on {self.server_name}")

    def create_subscription(self, subscription_name: str, connection_info: str, publication_name: str):
        sql = generate_create_subscription_query(subscription_name, connection_info, publication_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Subscription '{subscription_name}' created on {self.server_name}")

    def drop_subscription(self, subscription_name: str):
        sql = generate_drop_subscription_query(subscription_name)
        execute_sql(self.conn_params, sql, self.server_name)
        logger.debug(f"Subscription '{subscription_name}' dropped on {self.server_name}")
