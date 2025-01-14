# implementations/vanilla.py

import os
import subprocess
import sys
from implementations.base_ddl import BaseDDL
from sql.publication import generate_drop_publication_query
from sql.schema import generate_drop_schema_query
from sql.subscription import generate_drop_subscription_query
from utils.execute import execute_sql
from utils.log_handler import logger

class Vanilla(BaseDDL):
    """
    Vanilla realistaion without any patches, extenshions и т.д.
    """

    def build_source(self, clean: bool = False) -> None:
        logger.debug("[Vanilla] Building PostgreSQL from source if needed.")

        config = self.config
        pg_source_dir = config.pg_source_dir

        if not pg_source_dir or not os.path.exists(pg_source_dir):
            logger.error(f"[Vanilla] PostgreSQL source directory not found: {pg_source_dir}")
            sys.exit(1)

        try:
            if clean:
                logger.debug("[Vanilla] Running 'make clean'...")
                result = subprocess.run(['make', 'clean'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug("[Vanilla] Running './configure'...")
            result = subprocess.run(['./configure'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

            logger.debug("[Vanilla] Running 'make'...")
            result = subprocess.run(['make'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug("[Vanilla] Running 'make install'...")
            result = subprocess.run(['make', 'install'], cwd=pg_source_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)

            logger.debug("[Vanilla] PostgreSQL built and installed successfully.")

        except subprocess.CalledProcessError as e:
            logger.error(f"[Vanilla] Build failed: {e}")
            sys.exit(1)

    def setup_master(self, node_name: str, ddl: bool) -> None:

        logger.debug(f"[Vanilla] Setting up master '{node_name}' with ddl={False}...")
        master_cluster = self.config.get_cluster_by_name(node_name)

        self.create_schema(node_name, master_cluster.replication_schema)
        self.create_table(node_name, master_cluster.replication_schema, master_cluster.replication_table)
        publication_name = f"pub_{node_name}"
        self.create_publication(node_name, publication_name, master_cluster.replication_schema, False)

    def setup_replica(self, node_name: str, source_node_name: str, ddl: bool, cascade: bool) -> None:
        logger.debug(f"[Vanilla] No preparation needed for subscriber '{node_name}'.")
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

    def cleanup_cluster(self) -> None:
        logger.debug(f"[Vanilla] Cleaning up cluster.")
        nodes = self.config.nodes

        for node in nodes:
            server_name = node.name
            conn_params = node.conn_params.model_dump()
            subs = self.get_subscriptions(server_name)
            for (subname,) in subs:
                drop_sub_sql = generate_drop_subscription_query(subname)
                try:
                    execute_sql(conn_params, drop_sub_sql, server_name, autocommit=True)
                    logger.debug(f"Dropped subscription {subname} on {server_name}")
                except Exception as e:
                    logger.error(f"Failed to drop subscription {subname} on {server_name}: {e}")

        for node in nodes:
            server_name = node.name
            conn_params = node.conn_params.model_dump()
            pubs = self.get_publications(server_name)
            for (pubname,) in pubs:
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

        logger.debug("Replication cleanup completed on all nodes.")
