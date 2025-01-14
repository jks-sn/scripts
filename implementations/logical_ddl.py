# implementations/logical_ddl_ext.py

from implementations.base_ddl import BaseDDL
from utils.log_handler import logger
import subprocess
import sys

class LogicalDDLExt(BaseDDL):

    def __init__(self, config):
        super().__init__(config)
        self.logical_ddl_source_dir = config.logical_ddl_source_dir
        self.pg_bin_dir = config.pg_bin_dir

    def build_source(self) -> None:
        logger.debug("[LogicalDDLExt] Building 'logical_ddl' from sources if needed.")
        if not self.logical_ddl_source_dir:
            logger.error("[LogicalDDLExt] No 'logical_ddl_source_dir' specified in config.")
            sys.exit(1)
        try:
            cmd_make = ["make", f"PG_CONFIG={self.pg_bin_dir}/pg_config"]
            cmd_make_install = ["make", f"PG_CONFIG={self.pg_bin_dir}/pg_config", "install"]

            logger.debug(f"[LogicalDDLExt] Running 'make' in '{self.logical_ddl_source_dir}'...")
            subprocess.run(cmd_make, cwd=self.logical_ddl_source_dir, check=True)

            logger.debug(f"[LogicalDDLExt] Running 'make install' in '{self.logical_ddl_source_dir}'...")
            subprocess.run(cmd_make_install, cwd=self.logical_ddl_source_dir, check=True)

            logger.debug("[LogicalDDLExt] 'logical_ddl' extension built and installed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"[LogicalDDLExt] Build error: {e}")
            sys.exit(1)

    def prepare_master(self, cluster_name: str) -> None:
        logger.debug(f"[LogicalDDLExt] Preparing master '{cluster_name}'.")

        create_ext_sql = "CREATE EXTENSION IF NOT EXISTS logical_ddl;"
        self._execute(cluster_name, create_ext_sql)
        logger.debug(f"[LogicalDDLExt] Extension 'logical_ddl' created on '{cluster_name}'.")

        insert_settings_sql = "INSERT INTO logical_ddl.settings (publish, source) VALUES (true, 'source1') ON CONFLICT DO NOTHING;"
        self._execute(cluster_name, insert_settings_sql)
        logger.debug(f"[LogicalDDLExt] Configured '{cluster_name}' as publisher with source 'source1'.")

        schema_name = self.config.get_replication_schema(cluster_name)
        insert_tables_sql = f"""
        INSERT INTO logical_ddl.publish_tablelist (relid)
        SELECT c.oid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{{ schema_name }}'
        AND c.relkind = 'r';  -- Только обычные таблицы
        """.replace("{{ schema_name }}", schema_name)
        self._execute(cluster_name, insert_tables_sql)
        logger.debug(f"[LogicalDDLExt] All tables in schema '{schema_name}' added to 'publish_tablelist' on '{cluster_name}'.")

        publication_name = f"pub_{cluster_name}"
        alter_pub_sql = f"ALTER PUBLICATION {publication_name} ADD TABLE logical_ddl.shadow_table;"
        self._execute(cluster_name, alter_pub_sql)
        logger.debug(f"[LogicalDDLExt] 'shadow_table' added to publication '{publication_name}' on '{cluster_name}'.")

    def prepare_subscriber(self, cluster_name: str) -> None:
        logger.debug(f"[LogicalDDLExt] Preparing subscriber '{cluster_name}'.")

        create_ext_sql = "CREATE EXTENSION IF NOT EXISTS logical_ddl;"
        self._execute(cluster_name, create_ext_sql)
        logger.debug(f"[LogicalDDLExt] Extension 'logical_ddl' created on '{cluster_name}'.")

        insert_settings_sql = "INSERT INTO logical_ddl.settings (publish, source) VALUES (false, 'source1') ON CONFLICT DO NOTHING;"
        self._execute(cluster_name, insert_settings_sql)
        logger.debug(f"[LogicalDDLExt] Configured '{cluster_name}' as subscriber with source 'source1'.")

        schema_name = self.config.get_replication_schema(cluster_name)
        insert_tables_sql = f"""
        INSERT INTO logical_ddl.subscribe_tablelist (source, relid)
        SELECT 'source1', c.oid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{{ schema_name }}'
        AND c.relkind = 'r';  -- Только обычные таблицы
        """.replace("{{ schema_name }}", schema_name)
        self._execute(cluster_name, insert_tables_sql)
        logger.debug(f"[LogicalDDLExt] All tables in schema '{schema_name}' added to 'subscribe_tablelist' on '{cluster_name}'.")

    def cleanup_cluster(self, cluster_name: str) -> None:
        logger.debug(f"[LogicalDDLExt] Cleaning up cluster '{cluster_name}'.")

        super().cleanup_cluster(cluster_name)

        cluster = self.config.get_cluster_by_name(cluster_name)
        if cluster.role == "master":
            slots = execute_sql(self.cluster_conn[cluster_name], "SELECT slot_name FROM pg_replication_slots WHERE plugin = 'pgoutput';", cluster_name, fetch=True)
            for (slot_name,) in slots:
                drop_slot_sql = f"SELECT pg_drop_replication_slot('{slot_name}');"
                try:
                    self._execute(cluster_name, drop_slot_sql, autocommit=True)
                    logger.debug(f"[LogicalDDLExt] Dropped replication slot '{slot_name}' on '{cluster_name}'.")
                except Exception as e:
                    logger.error(f"[LogicalDDLExt] Cannot drop replication slot '{slot_name}' on '{cluster_name}': {e}")

        schema_name = self.config.get_replication_schema(cluster_name)
        if schema_name:
            drop_schema_sql = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
            self._execute(cluster_name, drop_schema_sql, autocommit=True)
            logger.debug(f"[LogicalDDLExt] Dropped schema '{schema_name}' on '{cluster_name}'.")
