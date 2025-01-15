# implementations/base_ddl.py

import grp
import os
import pwd
import subprocess
import sys

import click
from interfaces.ddl_interface import DDLInterface
from sql.publication import generate_create_publication_query, generate_drop_publication_query
from sql.schema import generate_create_schema_query, generate_drop_schema_query
from sql.subscription import generate_create_subscription_query, generate_drop_subscription_query
from sql.table import generate_add_column_query, generate_create_table_query, generate_drop_table_query
from utils.execute import execute_sql, run_as_postgres
from utils.log_handler import logger
from typing import List, Dict

class BaseDDL(DDLInterface):

    LOG_TAG = "[BaseDDL]"

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
        logger.debug(f"{self.LOG_TAG} Publication '{publication_name}' created on '{node_name}' with ddl={ddl}.")

    def drop_publication(self, node_name: str, publication_name: str) -> None:
        sql = generate_drop_publication_query(publication_name)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Publication '{publication_name}' dropped on '{node_name}'.")

    def create_subscription(self, node_name: str, subscription_name: str, connection_info: str, publication_name: str) -> None:
        sql = generate_create_subscription_query(subscription_name, connection_info, publication_name)
        self._execute(node_name, sql, autocommit=True)
        logger.debug(f"{self.LOG_TAG} Subscription '{subscription_name}' created on '{node_name}'.")

    def drop_subscription(self, node_name: str, subscription_name: str) -> None:
        sql = generate_drop_subscription_query(subscription_name)
        self._execute(node_name, sql, autocommit=True)
        logger.debug(f"{self.LOG_TAG} Subscription '{subscription_name}' dropped on '{node_name}'.")

    def create_schema(self, node_name: str, schema_name: str) -> None:
        sql = generate_create_schema_query(schema_name)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Schema '{schema_name}' created on '{node_name}'.")

    def drop_schema(self, node_name: str, schema_name: str) -> None:
        sql = generate_drop_schema_query(schema_name)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Schema '{schema_name}' dropped on '{node_name}'.")

    def create_table(self, node_name: str, schema_name: str, table_name: str) -> None:
        sql = generate_create_table_query(schema_name, table_name)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Table '{schema_name}.{table_name}' created on '{node_name}'.")

    def drop_table(self, node_name: str, schema_name: str, table_name: str) -> None:
        sql = generate_drop_table_query(schema_name, table_name)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Table '{schema_name}.{table_name}' dropped on '{node_name}'.")

    def add_column(self, node_name: str, schema_name: str, table_name: str,
                   column_name: str, column_type: str = "INTEGER", default_value=None) -> None:
        sql = generate_add_column_query(schema_name, table_name, column_name, column_type, default_value)
        self._execute(node_name, sql)
        logger.debug(f"{self.LOG_TAG} Added column '{column_name}' to '{schema_name}.{table_name}' on '{node_name}'.")

    def table_exists(self, node_name: str, schema_name: str, table_name: str) -> bool:
        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='{schema_name}' AND table_name='{table_name}'
        );
        """
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        exists = bool(results) and results[0][0]
        logger.debug(f"{self.LOG_TAG} Table '{schema_name}.{table_name}' exists on '{node_name}': {exists}")
        return exists

    def get_table_columns(self, node_name: str, schema_name: str, table_name: str) -> List[Dict]:
        sql = f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema='{schema_name}' AND table_name='{table_name}';
        """
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        columns = [{"column_name": row[0], "data_type": row[1], "default": row[2]} for row in results]
        logger.debug(f"{self.LOG_TAG} Columns for table '{schema_name}.{table_name}' on '{node_name}': {columns}")
        return columns

    # --- Help methods ---

    def get_subscriptions(self, node_name: str) -> List[str]:
        """
        Get name of all subscriptions in node
        """
        sql = "SELECT subname FROM pg_subscription;"
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        subs = [row[0] for row in results]
        logger.debug(f"{self.LOG_TAG} Subscriptions on '{node_name}': {subs}")
        return subs

    def get_publications(self, node_name: str) -> List[str]:
        """
        Получает все имена публикаций на указанном кластере.
        """
        sql = "SELECT pubname FROM pg_publication;"
        results = execute_sql(self.node_conn[node_name], sql, node_name, fetch=True)
        pubs = [row[0] for row in results]
        logger.debug(f"{self.LOG_TAG} Publications on '{node_name}': {pubs}")
        return pubs

    # --- interface realisations ---

    def build_source(self, clean: bool = False) -> None:
        logger.debug(f"{self.LOG_TAG} Building PostgreSQL from source if needed.")

        config = self.config
        pg_source_dir = config.pg_source_dir

        if not pg_source_dir or not os.path.exists(pg_source_dir):
            logger.error(f"{self.LOG_TAG} PostgreSQL source directory not found: {pg_source_dir}")
            sys.exit(1)

        try:
            if clean:
                logger.debug("{self.LOG_TAG} Running 'make clean'...")
                result = subprocess.run(['make', 'clean'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug("{self.LOG_TAG} Running './configure'...")
            result = subprocess.run(['./configure'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

            logger.debug("{self.LOG_TAG} Running 'make'...")
            result = subprocess.run(['make'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug("{self.LOG_TAG} Running 'make install'...")
            result = subprocess.run(['make', 'install'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug(f"{self.LOG_TAG} PostgreSQL built and installed successfully.")
            click.echo(f"{self.LOG_TAG} PostgreSQL built and installed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.LOG_TAG} Build failed: {e}")
            sys.exit(1)

    def init_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Init cluster starting...")
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

                self.ensure_server_stopped(node_name)

                if os.path.exists(data_dir):
                    logger.debug(f"{self.LOG_TAG} Removing old data dir '{data_dir}' for node '{node_name}'...")
                    subprocess.run(["rm", "-rf", data_dir], check=True)

                logger.debug(f"Creating new data dir '{data_dir}' for node '{node_name}'...")
                os.makedirs(data_dir, exist_ok=True)

                logger.debug(f"{self.LOG_TAG} Changing ownership of data dir '{data_dir}' to postgres:postgres")
                os.chown(data_dir, uid, gid)

                logger.debug(f"{self.LOG_TAG} Initializing node '{node_name}' with initdb => {data_dir}")
                run_as_postgres([initdb_path, "-D", data_dir])

                conf_path = os.path.join(data_dir, 'postgresql.conf')
                with open(conf_path, 'a') as conf_file:
                    conf_file.write(f"\n# Settings for node {node_name}\n")
                    conf_file.write(f"port = {port}\n")
                    conf_file.write("wal_level = logical\n")
                    conf_file.write("max_wal_senders = 10\n")
                    conf_file.write("max_replication_slots = 10\n")
                    conf_file.write("logging_collector = on\n")
                    conf_file.write("log_directory = 'log'\n")
                    conf_file.write("log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'\n")

                log_dir = os.path.join(data_dir, 'log')
                os.makedirs(log_dir, exist_ok=True)
                os.chown(log_dir, uid, gid)

                logger.debug(f"{self.LOG_TAG} Node {node_name} have been initialized successfully.")
            logger.debug(f"{self.LOG_TAG} cluster have been initialized successfully.")

        except Exception as e:
            logger.error(f"{self.LOG_TAG} Error initializing cluster nodes: {e}")
            sys.exit(1)

    def setup_master(self, node_name: str, ddl: bool) -> None:

        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with ddl={ddl}...")

        master_cluster = self.config.get_cluster_by_name(node_name)

        self.create_schema(node_name, master_cluster.replication_schema)
        self.create_table(node_name, master_cluster.replication_schema, master_cluster.replication_table)
        publication_name = f"pub_{node_name}"
        self.create_publication(node_name, publication_name, master_cluster.replication_schema, ddl)

        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with ddl={ddl} finish successfully")

    def setup_replica(self, node_name: str, source_node_name: str, ddl: bool, cascade: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with ddl={ddl}...")

        replica_server = self.config.get_cluster_by_name(node_name)
        master_server = self.config.get_cluster_by_name(source_node_name)

        self.create_schema(node_name, replica_server.replication_schema)
        self.create_table(node_name, replica_server.replication_schema, replica_server.replication_table)

        connection_info = (
            f"host={master_server.conn_params.host} "
            f"port={master_server.conn_params.port} "
            f"dbname={master_server.conn_params.dbname} "
            f"user={master_server.conn_params.user} "
            f"password={master_server.conn_params.password}"
        )
        subscription_name = f"sub_{node_name}"
        publication_name = f"pub_{source_node_name}"

        self.create_subscription(node_name, subscription_name, connection_info, publication_name)

        if cascade:
            cascade_publication_name = f"pub_{node_name}"
            self.create_publication(node_name, cascade_publication_name, replica_server.replication_schema, False)

        logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with ddl={ddl} finish successfully")

    def cleanup_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Cleaning up cluster.")
        nodes = self.config.nodes

        for node in nodes:
            server_name = node.name
            conn_params = node.conn_params.model_dump()
            subname = f"sub_{server_name}"
            drop_sub_sql = generate_drop_subscription_query(subname)
            try:
                execute_sql(conn_params, drop_sub_sql, server_name, autocommit=True)
                logger.debug(f"Dropped subscription {subname} on {server_name}")
            except Exception as e:
                logger.error(f"Failed to drop subscription {subname} on {server_name}: {e}")

        for node in nodes:
            server_name = node.name
            conn_params = node.conn_params.model_dump()
            pubname = f"pub_{server_name}"
            drop_pub_sql = generate_drop_publication_query(pubname);
            try:
                execute_sql(conn_params, drop_pub_sql, server_name)
                logger.debug(f"Dropped publication {pubname} on {server_name}")
            except Exception as e:
                logger.error(f"Failed to drop publication {pubname} on {server_name}: {e}")

        for node in nodes:
            server_name = node.name
            conn_params = node.conn_params.model_dump()
            schema_name = node.replication_schema
            drop_schema_sql = generate_drop_schema_query(schema_name)
            try:
                execute_sql(conn_params, drop_schema_sql, server_name)
                logger.debug(f"Dropped schema {schema_name} on {server_name}")
            except Exception as e:
                logger.error(f"Failed to drop schema {schema_name} on {server_name}: {e}")

        logger.debug(f"{self.LOG_TAG} Replication cleanup completed on all nodes.")

    def start_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Starting all cluster nodes.")
        try:
            for node in self.config.nodes:
                node_name = node.name
                if not self.is_server_running(node_name):
                    self.start_server(node_name)
                else:
                    logger.debug(f"{self.LOG_TAG} Server '{node_name}' is already running.")
            logger.debug(f"{self.LOG_TAG} All cluster nodes have been started successfully.")
        except Exception as e:
            logger.error(f"{self.LOG_TAG} Error starting cluster nodes: {e}")
            sys.exit(1)

    def start_server(self, node_name: str) -> None:
        pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
        data_dir = os.path.join(self.config.pg_cluster_dir, node_name)
        log_file = os.path.join(data_dir, 'log', 'postgresql.log')
        try:
            logger.debug(f"{self.LOG_TAG} Starting server '{node_name}'...")
            run_as_postgres([pg_ctl_path, '-D', data_dir, '-l', log_file, 'start'])
            logger.debug(f"{self.LOG_TAG} Server '{node_name}' started successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.LOG_TAG} Failed to start server '{node_name}': {e}")
            sys.exit(1)

    def status_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Checking status of all cluster nodes.")

        try:
            for node in self.config.nodes:
                node_name = node.name
                running = self.is_server_running(node_name)
                status = "running" if running else "stopped"
                logger.info(f"Cluster '{node_name}': {status}")
            logger.debug(f"{self.LOG_TAG} Status check completed for all cluster nodes.")
        except Exception as e:
            logger.error(f"{self.LOG_TAG} Error checking status of cluster nodes: {e}")
            sys.exit(1)

    def is_server_running(self, node_name: str) -> bool:
        pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
        data_dir = os.path.join(self.config.pg_cluster_dir, node_name)
        try:
            result = run_as_postgres([pg_ctl_path, '-D', data_dir, 'status'], suppress_output=True)
            logger.debug(f"{self.LOG_TAG} Server status for '{node_name}': Running.")
            return True
        except subprocess.CalledProcessError:
            logger.debug(f"{self.LOG_TAG} Server status for '{node_name}': Not running.")
            return False

    def stop_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Stopping all cluster nodes.")
        try:
            for node in self.config.nodes:
                self.stop_server(node.name)
        except Exception as e:
            logger.error(f"{self.LOG_TAG} Error stopping cluster nodes: {e}")
            sys.exit(1)

    def stop_server(self, node_name: str) -> None:
        pg_ctl_path = os.path.join(self.config.pg_bin_dir, 'pg_ctl')
        data_dir = os.path.join(self.config.pg_cluster_dir, node_name)
        try:
            logger.debug(f"{self.LOG_TAG} Stopping server '{node_name}'...")
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop'])
            logger.debug(f"{self.LOG_TAG} Server '{node_name}' stopped successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.LOG_TAG} Failed to stop server '{node_name}': {e}")
            sys.exit(1)

    def ensure_server_stopped(self, node_name: str) -> None:
        if self.is_server_running(node_name):
            self.stop_server(node_name)
        else:
            logger.debug(f"{self.LOG_TAG} Server '{node_name}' is already stopped.")
