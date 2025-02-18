# implementations/pgl_ddl_deploy.py

import os
import subprocess
import sys
from implementations.base_ddl import BaseDDL
from utils.execute import execute_sql
from utils.log_handler import logger

class PG_DDL_Deploy(BaseDDL):

    LOG_TAG = "[PGL_DDL_DEPLOY]"

    def __init__(self, config):
        super().__init__(config)

    def build_source(self, clean: bool = False) -> None:
        logger.debug(f"{self.LOG_TAG} Building PostgreSQL from source.")
        super().build_source(clean=clean)
        logger.debug(f"{self.LOG_TAG} Building 'pgl_ddl_deploy' extension from sources.")

        pg_config_path = os.path.join(self.config.pg_bin_dir, "pg_config")
        pg_pglogical_dir = os.path.join(self.config.pg_source_dir, "contrib", "pglogical")
        pgl_pgl_ddl_deploy_dir = os.path.join(self.config.pg_source_dir, "contrib", "pgl_ddl_deploy")
        try:
            if clean:
                logger.debug(f"{self.LOG_TAG} Running 'make clean' in '{pg_pglogical_dir}'...")
                subprocess.run(
                    ["make", f"PG_CONFIG={pg_config_path}", "clean"],
                    cwd=pg_pglogical_dir, check=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
                )

                logger.debug(f"{self.LOG_TAG} Running 'make clean' in '{pgl_pgl_ddl_deploy_dir}'...")
                subprocess.run(
                    ["make", f"PG_CONFIG={pg_config_path}", "clean"],
                    cwd=pgl_pgl_ddl_deploy_dir, check=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
                )

            logger.debug(f"{self.LOG_TAG} Running 'make' in '{pg_pglogical_dir}'...")
            subprocess.run(
                ["make", f"PG_CONFIG={pg_config_path}"],
                cwd=pg_pglogical_dir, check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            logger.debug(f"{self.LOG_TAG} Running 'make install' in '{pg_pglogical_dir}'...")
            subprocess.run(
                ["make", "install", f"PG_CONFIG={pg_config_path}"],
                cwd=pg_pglogical_dir, check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            logger.debug(f"{self.LOG_TAG} Running 'make' in '{pgl_pgl_ddl_deploy_dir}'...")
            subprocess.run(
                ["make", f"PG_CONFIG={pg_config_path}"],
                cwd=pgl_pgl_ddl_deploy_dir, check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            logger.debug(f"{self.LOG_TAG} Running 'make install' in '{pgl_pgl_ddl_deploy_dir}'...")
            subprocess.run(
                ["make", "install", f"PG_CONFIG={pg_config_path}"],
                cwd=pgl_pgl_ddl_deploy_dir, check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            logger.debug(f"{self.LOG_TAG} 'pglogical and pgl_ddl_deploy' extensions built and installed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.LOG_TAG} Build error: {e}")
            sys.exit(1)

    def init_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Initializing cluster with pgl_ddl_deploy extension...")
        super().init_cluster()

        for node in self.config.nodes:
            node_name = node.name
            data_dir = os.path.join(self.config.pg_cluster_dir, node_name)
            conf_path = os.path.join(data_dir, 'postgresql.conf')

            logger.debug(f"{self.LOG_TAG} Appending pglogical to shared_preload_libraries in {conf_path} for node '{node_name}'")
            with open(conf_path, 'a') as conf:
                conf.write("\n# Additional settings for pglogical from pgl_ddl_deploy\n")
                conf.write("shared_preload_libraries = 'pglogical'\n")

        for node in self.config.nodes:
            node_name = node.name
            logger.debug(f"{self.LOG_TAG} Starting server '{node_name}' again with updated postgresql.conf...")
            self.start_server(node_name)
            try:
                self._execute(node_name, "CREATE EXTENSION IF NOT EXISTS pglogical;")
                self._execute(node_name, "CREATE EXTENSION IF NOT EXISTS pgl_ddl_deploy;")

                self._execute(node_name, f"SELECT pgl_ddl_deploy.add_role(oid) FROM pg_roles WHERE rolname = '{node.replication_user}';")

                logger.debug(f"{self.LOG_TAG} Extension pgl_ddl_deploy created on '{node_name}'.")
            except Exception as e:
                logger.error(f"{self.LOG_TAG} Failed to create extension on '{node_name}': {e}")
                sys.exit(1)
            self.stop_server(node_name)
        logger.debug(f"{self.LOG_TAG} Cluster initialized successfully with pgl_ddl_deploy.")

    def setup_master(self, node_name: str, ddl: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with pglogical DDL replication enabled.")

        node = self.config.get_node_by_name(node_name)

        self.create_schema(node_name, node.replication_schema)
        self.create_table(node_name, node.replication_schema, node.replication_table)

        dsn = (
            f"host={node.conn_params.host} port={node.conn_params.port} "
            f"dbname={node.conn_params.dbname} user={node.conn_params.user} "
            f"password={node.conn_params.password}"
        )
        create_node_sql = f"""
            SELECT pglogical.create_node(
                node_name := 'provider_{node_name}',
                dsn := '{dsn}'
            );
        """
        self._execute(node_name, create_node_sql)

        add_tables_sql = f"""
            SELECT pglogical.replication_set_add_all_tables(
                'default',
                ARRAY['{node.replication_schema}'],
                false
            );
        """
        self._execute(node_name, add_tables_sql)

        set_config_sql = f"""
        INSERT INTO pgl_ddl_deploy.set_configs
            (set_name, include_schema_regex, lock_safe_deployment, allow_multi_statements, driver)
        VALUES (
            'pub_{node.name}',
            '^{node.replication_schema}$',
            true,
            false,
            'pglogical'
        );
        """
        self._execute(node_name, set_config_sql)

        self._execute(node_name, f"SELECT pgl_ddl_deploy.deploy('pub_{node.name}');")
        logger.debug(f"{self.LOG_TAG} Master '{node_name}' configured for DDL replication in '{node.replication_schema}'.")

    def setup_replica(self, node_name: str, master_node_name: str, ddl: bool, cascade: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}', source='{master_node_name}', using pglogical DDL replication...")
        replica_node = self.config.get_node_by_name(node_name)
        master_node = self.config.get_node_by_name(master_node_name)
        self.create_schema(node_name, replica_node.replication_schema)
        self.create_table(node_name, replica_node.replication_schema, replica_node.replication_table)

        replica_dsn = (
            f"host={replica_node.conn_params.host} port={replica_node.conn_params.port} "
            f"dbname={replica_node.conn_params.dbname} user={replica_node.conn_params.user} "
            f"password={replica_node.conn_params.password}"
        )
        create_subscriber_node_sql = f"""
            SELECT pglogical.create_node(
                node_name := 'subscriber_{node_name}',
                dsn := '{replica_dsn}'
            );
        """
        self._execute(node_name, create_subscriber_node_sql)

        master_dsn = (
            f"host={master_node.conn_params.host} port={master_node.conn_params.port} "
            f"dbname={master_node.conn_params.dbname} user={master_node.conn_params.user} "
            f"password={master_node.conn_params.password}"
        )

        create_subscription_sql = f"""
            SELECT pglogical.create_subscription(
                subscription_name := 'sub_{node_name}',
                provider_dsn := '{master_dsn}',
            );
        """
        self._execute(node_name, create_subscription_sql)

        if cascade:
            logger.debug(f"{self.LOG_TAG} cascade=True: creating additional publication (replication set) on subscriber '{node_name}' for further chaining.")

        logger.debug(f"{self.LOG_TAG} Replica '{node_name}' successfully subscribed to pglogical provider '{master_node_name}' with DDL replication.")


    def cleanup_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Cleaning up cluster with pgl_ddl_deploy.")

        for node in self.config.nodes:
            node_name = node.name
            try:
                drop_sub_sql = f"SELECT pglogical.drop_subscription('sub_{node_name}', true);"
                self._execute(node_name, drop_sub_sql)
            except Exception as e:
                logger.debug(f"{self.LOG_TAG} Subscription sub_{node_name} was not dropped or did not exist: {e}")

            try:
                drop_node_sql = f"SELECT pglogical.drop_node('subscriber_{node_name}', true);"
                self._execute(node_name, drop_node_sql)
                drop_node_sql2 = f"SELECT pglogical.drop_node('provider_{node_name}', true);"
                self._execute(node_name, drop_node_sql2)
            except Exception as e:
                logger.debug(f"{self.LOG_TAG} pglogical node not removed or not existed: {e}")

        for node in self.config.nodes:
            node_name = node.name
            try:
                self._execute(node_name, "DROP EXTENSION IF EXISTS pgl_ddl_deploy CASCADE;")
                self._execute(node_name, "CREATE EXTENSION IF NOT EXISTS pgl_ddl_deploy;")
                logger.debug(f"{self.LOG_TAG} Dropped and re-created extension pgl_ddl_deploy on '{node_name}'.")
            except Exception as e:
                logger.error(f"{self.LOG_TAG} Failed to drop or recreate extension pgl_ddl_deploy on '{node_name}': {e}")

        super().cleanup_cluster()
        logger.debug(f"{self.LOG_TAG} Cleanup for pgl_ddl_deploy completed on all nodes.")
