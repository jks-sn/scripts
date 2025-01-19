# implementations/logical_ddl_ext.py

import os
from implementations.base_ddl import BaseDDL
from utils.execute import execute_sql
from utils.log_handler import logger
import subprocess
import sys

class LogicalDDLExt(BaseDDL):

    LOG_TAG = "[LOGICALDDLEXT]"

    def __init__(self, config):
        super().__init__(config)

    def build_source(self, clean: bool = False) -> None:
        logger.debug("{self.LOG_TAG} Building PostgreSQL from source if needed.")
        super().build_source(clean=clean)
        logger.debug(f"{self.LOG_TAG} Building 'logical_ddl' from sources if needed.")

        pg_config_path = os.path.join(self.config.pg_bin_dir, "pg_config")
        logical_ddl_source_dir = os.path.join(self.config.pg_source_dir, "contrib", "logical_ddl")
        try:
            if clean:
                logger.debug(f"{self.LOG_TAG} Running 'make clean' in '{logical_ddl_source_dir}'...")
                cmd_make_clean = ["make", f"PG_CONFIG={pg_config_path}", "clean"]
                subprocess.run(cmd_make_clean, cwd=logical_ddl_source_dir, check=True)

            logger.debug(f"{self.LOG_TAG} Running 'make' in '{logical_ddl_source_dir}'...")
            cmd_make = ["make", f"PG_CONFIG={pg_config_path}"]
            subprocess.run(cmd_make, cwd=logical_ddl_source_dir, check=True)

            logger.debug(f"{self.LOG_TAG} Running 'make install' in '{logical_ddl_source_dir}'...")
            cmd_make_install = ["make", f"PG_CONFIG={pg_config_path}", "install"]
            subprocess.run(cmd_make_install, cwd=logical_ddl_source_dir, check=True)

            logger.debug(f"{self.LOG_TAG} 'logical_ddl' extension built and installed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.LOG_TAG} Build error: {e}")
            sys.exit(1)

    def init_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Init cluster starting with logical_ddl extension build...")
        super().init_cluster()
        logger.debug(f"{self.LOG_TAG} Creating extension logical_ddl on each node if needed...")
        for node in self.config.nodes:
            node_name = node.name
            self.start_server(node_name)
            try:
                create_ext_sql = "CREATE EXTENSION IF NOT EXISTS logical_ddl;"
                self._execute(node_name, create_ext_sql)
                logger.debug(f"{self.LOG_TAG} Extension logical_ddl created on '{node_name}'.")
            except Exception as e:
                logger.error(f"{self.LOG_TAG} Failed to create extension on '{node_name}': {e}")
                sys.exit(1)
            self.stop_server(node.name)

        logger.debug(f"{self.LOG_TAG} cluster have been initialized successfully.")

    def setup_master(self, node_name: str, ddl: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with ddl={False}...")
        super().setup_master(node_name=node_name, ddl=False)

        set_publisher_sql = f"""
            INSERT INTO logical_ddl.settings (publish, source)
            VALUES (true, '{node_name}')
            ON CONFLICT DO NOTHING
        """
        self._execute(node_name, set_publisher_sql)
        logger.debug(f"{self.LOG_TAG} Configured node '{node_name}' as publisher with source='{node_name}'.")

        add_tables_to_publish_sql = f"""
            INSERT INTO logical_ddl.publish_tablelist (relid)
            SELECT prrelid
            FROM pg_catalog.pg_publication_rel
            WHERE prpubid = (
                SELECT oid
                FROM pg_catalog.pg_publication
                WHERE pubname = 'pub_{node_name}'
            )
            ON CONFLICT DO NOTHING;
        """
        self._execute(node_name, add_tables_to_publish_sql)
        logger.debug(f"{self.LOG_TAG} Added tables from publication 'pub_{node_name}' "
                    "to 'logical_ddl.publish_tablelist'.")

        add_shadow_table_sql = f"""
            ALTER PUBLICATION pub_{node_name} ADD TABLE logical_ddl.shadow_table;
        """
        self._execute(node_name, add_shadow_table_sql)
        logger.debug(f"{self.LOG_TAG} Added 'logical_ddl.shadow_table' to publication 'pub_{node_name}'.")

        logger.debug(f"{self.LOG_TAG} Master '{node_name}' setup with ddl={False} finished successfully.")

    def setup_replica(self, node_name: str, master_node_name: str, ddl: bool, cascade: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Settuing up replica '{node_name} with master '{master_node_name}'.")
        super().setup_replica(node_name=node_name, master_node_name=master_node_name, ddl=False, cascade=cascade)
        set_subscriber_sql = f"""
            INSERT INTO logical_ddl.settings (publish, source)
            VALUES (false, '{master_node_name}')
            ON CONFLICT DO NOTHING
        """
        self._execute(node_name, set_subscriber_sql)
        logger.debug(f"{self.LOG_TAG} Configured node '{node_name}' as subscriber for source='{master_node_name}'.")

        add_tables_to_subscribe_sql = f"""
            INSERT INTO logical_ddl.subscribe_tablelist (source, relid)
            SELECT '{master_node_name}', srrelid
            FROM pg_catalog.pg_subscription_rel
            WHERE srsubid = (
                SELECT oid FROM pg_catalog.pg_subscription
                WHERE subname = 'sub_{node_name}'
            )
            ON CONFLICT DO NOTHING;
        """
        self._execute(node_name, add_tables_to_subscribe_sql)
        logger.debug(f"{self.LOG_TAG} Added tables from subscription 'sub_{node_name}' to 'logical_ddl.subscribe_tablelist'.")

        logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with ddl={False} finish successfully.")



    def cleanup_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Cleaning up cluster for logical_ddl extension...")

        super().cleanup_cluster()

        for node in self.config.nodes:
            node_name = node.name
            try:
                drop_settings_sql = "DELETE FROM logical_ddl.settings;"
                self._execute(node_name, drop_settings_sql)
                logger.debug(f"{self.LOG_TAG} Deleted records from logical_ddl.settings on '{node_name}'.")

                drop_publish_table_sql = "DELETE FROM logical_ddl.publish_tablelist;"
                self._execute(node_name, drop_publish_table_sql)
                logger.debug(f"{self.LOG_TAG} Deleted records from logical_ddl.publish_tablelist on '{node_name}'.")

                drop_subscribe_table_sql = "DELETE FROM logical_ddl.subscribe_tablelist;"
                self._execute(node_name, drop_subscribe_table_sql)
                logger.debug(f"{self.LOG_TAG} Deleted records from logical_ddl.subscribe_tablelist on '{node_name}'.")
            except Exception as e:
                logger.error(f"{self.LOG_TAG} Failed to cleanup logical_ddl.* tables on '{node_name}': {e}")

        logger.debug(f"{self.LOG_TAG} Cleanup for logical_ddl extension completed on all nodes.")
