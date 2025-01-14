# implementations/base_ddl.py

import grp
import os
import pwd
import subprocess
import sys
from interfaces.ddl_interface import DDLInterface
from sql.publication import generate_create_publication_query, generate_drop_publication_query
from sql.schema import generate_create_schema_query, generate_drop_schema_query
from sql.subscription import generate_create_subscription_query, generate_drop_subscription_query
from sql.table import generate_add_column_query, generate_create_table_query, generate_drop_table_query
from utils.execute import execute_sql, run_as_postgres
from utils.log_handler import logger
from typing import List, Dict

class BaseDDL(DDLInterface):

    def __init__(self, config):
        """
        :param config: Объект конфигурации, содержащий детали кластеров.
        """
        self.config = config
        self.node_conn = {node.name: node.conn_params.model_dump() for node in config.nodes}

    def _execute(self, node_name: str, sql: str, autocommit: bool = False):
        conn_params = self.node_conn[node_name]
        execute_sql(conn_params, sql, server_name=node_name, autocommit=autocommit)

    def create_publication(self, node_name: str, publication_name: str, schema_name: str, ddl: bool) -> None:
        sql = generate_create_publication_query(publication_name, schema_name, ddl)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Publication '{publication_name}' created on '{node_name}' with ddl={ddl}.")

    def drop_publication(self, node_name: str, publication_name: str) -> None:
        sql = generate_drop_publication_query(publication_name)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Publication '{publication_name}' dropped on '{node_name}'.")

    def create_subscription(self, node_name: str, subscription_name: str, connection_info: str, publication_name: str) -> None:
        sql = generate_create_subscription_query(subscription_name, connection_info, publication_name)
        self._execute(node_name, sql, autocommit=True)
        logger.debug(f"[BaseDDL] Subscription '{subscription_name}' created on '{node_name}'.")

    def drop_subscription(self, node_name: str, subscription_name: str) -> None:
        sql = generate_drop_subscription_query(subscription_name)
        self._execute(node_name, sql, autocommit=True)
        logger.debug(f"[BaseDDL] Subscription '{subscription_name}' dropped on '{node_name}'.")

    def create_schema(self, node_name: str, schema_name: str) -> None:
        sql = generate_create_schema_query(schema_name)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Schema '{schema_name}' created on '{node_name}'.")

    def drop_schema(self, node_name: str, schema_name: str) -> None:
        sql = generate_drop_schema_query(schema_name)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Schema '{schema_name}' dropped on '{node_name}'.")

    def create_table(self, node_name: str, schema_name: str, table_name: str) -> None:
        sql = generate_create_table_query(schema_name, table_name)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Table '{schema_name}.{table_name}' created on '{node_name}'.")

    def drop_table(self, node_name: str, schema_name: str, table_name: str) -> None:
        sql = generate_drop_table_query(schema_name, table_name)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Table '{schema_name}.{table_name}' dropped on '{node_name}'.")

    def add_column(self, node_name: str, schema_name: str, table_name: str,
                   column_name: str, column_type: str = "INTEGER", default_value=None) -> None:
        sql = generate_add_column_query(schema_name, table_name, column_name, column_type, default_value)
        self._execute(node_name, sql)
        logger.debug(f"[BaseDDL] Added column '{column_name}' to '{schema_name}.{table_name}' on '{node_name}'.")

    def table_exists(self, node_name: str, schema_name: str, table_name: str) -> bool:
        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='{schema_name}' AND table_name='{table_name}'
        );
        """
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        exists = bool(results) and results[0][0]
        logger.debug(f"[BaseDDL] Table '{schema_name}.{table_name}' exists on '{node_name}': {exists}")
        return exists

    def get_table_columns(self, node_name: str, schema_name: str, table_name: str) -> List[Dict]:
        sql = f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema='{schema_name}' AND table_name='{table_name}';
        """
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        columns = [{"column_name": row[0], "data_type": row[1], "default": row[2]} for row in results]
        logger.debug(f"[BaseDDL] Columns for table '{schema_name}.{table_name}' on '{node_name}': {columns}")
        return columns

    # --- Help methods ---

    def get_subscriptions(self, node_name: str) -> List[str]:
        """
        Get name of all subscriptions in node
        """
        sql = "SELECT subname FROM pg_subscription;"
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        subs = [row[0] for row in results]
        logger.debug(f"[BaseDDL] Subscriptions on '{node_name}': {subs}")
        return subs

    def get_publications(self, node_name: str) -> List[str]:
        """
        Получает все имена публикаций на указанном кластере.
        """
        sql = "SELECT pubname FROM pg_publication;"
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        pubs = [row[0] for row in results]
        logger.debug(f"[BaseDDL] Publications on '{node_name}': {pubs}")
        return pubs

    # --- node methods ---

    def init_cluster(self) -> None:
        try:
            pg_bin_dir = self.config.pg_bin_dir
            pg_cluster_dir = self.config.pg_cluster_dir
            nodes = self.config.nodes

            initdb_path = os.path.join(pg_bin_dir, 'initdb')
            uid = pwd.getpwnam("postgres").pw_uid
            gid = grp.getgrnam("postgres").gr_gid

            for node in nodes:
                node_name = node.name
                data_dir = os.path.join(pg_cluster_dir, node_name)
                port = node.port

                if os.path.exists(data_dir):
                    logger.debug(f"Removing old data dir '{data_dir}' for node '{node_name}'...")
                    subprocess.run(["rm", "-rf", data_dir], check=True)

                logger.debug(f"Creating new data dir '{data_dir}' for node '{node_name}'...")
                os.makedirs(data_dir, exist_ok=True)

                logger.debug(f"Chown data dir '{data_dir}' to postgres:postgres")
                os.chown(data_dir, uid, gid)

                logger.debug(f"Initializing node '{node_name}' with initdb => {data_dir}")
                run_as_postgres([initdb_path, "-D", data_dir])

                conf_path = os.path.join(data_dir, 'postgresql.conf')
                with open(conf_path, 'a') as conf_file:
                    conf_file.write(f"\n# Settings for node {node_name}\n")
                    conf_file.write(f"port = {port}\n")
                    conf_file.write("wal_level = logical\n")
                    conf_file.write("max_wal_senders = 10\n")
                    conf_file.write("max_replication_slots = 10\n")
                    conf_file.write("logging_collector = on\n")

            logger.debug("node have been initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing node: {e}")
            sys.exit(1)

    def start_cluster(self) -> None:
        logger.debug("[BaseDDL] Starting all cluster nodes.")

        pg_cluster_dir = self.config.pg_cluster_dir
        try:
            for node in self.config.nodes:
                node_name = node.name
                data_dir = os.path.join(pg_cluster_dir, node_name)

                logger.debug(f"[BaseDDL] Starting node '{node_name}'...")
                pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
                run_as_postgres([pg_ctl_path, '-D', data_dir, 'start'], suppress_output=False)

            logger.debug("All cluster nodes have been started successfully.")
        except Exception as e:
            logger.error(f"[BaseDDL] Error starting cluster nodes: {e}")
            sys.exit(1)

    def stop_cluster(self) -> None:
        logger.debug("[BaseDDL] Stopping all cluster nodes.")

        pg_cluster_dir = self.config.pg_cluster_dir
        try:
            for node in self.config.nodes:
                node_name = node.name
                data_dir = os.path.join(pg_cluster_dir, node_name)

                logger.debug(f"[BaseDDL] Stopping node '{node_name}'...")
                pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
                run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop'], suppress_output=False)

            logger.debug("All cluster nodes have been stopped successfully.")
        except Exception as e:
            logger.error(f"[BaseDDL] Error stopping cluster nodes: {e}")
            sys.exit(1)

    def status_cluster(self) -> None:
        logger.debug("[BaseDDL] Checking status of all cluster nodes.")
        pg_cluster_dir = self.config.pg_cluster_dir

        try:
            for node in self.config.nodes:
                node_name = node.name
                data_dir = os.path.join(pg_cluster_dir, node_name)

                logger.debug(f"[BaseDDL] Checking status for node '{node_name}'...")
                pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
                run_as_postgres([pg_ctl_path, '-D', data_dir, 'status'], suppress_output=False)

            logger.debug("Status check completed for all cluster nodes.")
        except Exception as e:
            logger.error(f"[BaseDDL] Error checking status of cluster nodes: {e}")
            sys.exit(1)