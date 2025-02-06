# implementations/logical_ddl.py

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

		set_publisher_sql = f"""
			INSERT INTO logical_ddl.settings (publish, source)
			VALUES (true, '{node_name}')
		"""
		self._execute(node_name, set_publisher_sql)
		logger.debug(f"{self.LOG_TAG} Configured node '{node_name}' as publisher with source='{node_name}'.")

		node = self.config.get_node_by_name(node_name)
		pub_sql = f"CREATE PUBLICATION pub_{node_name};"
		self._execute(node_name, pub_sql)
		logger.debug(f"{self.LOG_TAG} Created publication 'pub_{node_name}' WITHOUT any tables.")

		self.create_schema(node_name, node.replication_schema)

		self.create_table(node_name, node.replication_schema, node.replication_table)

		add_shadow_table_sql = f"""
			ALTER PUBLICATION pub_{node_name} ADD TABLE logical_ddl.shadow_table;
		"""
		self._execute(node_name, add_shadow_table_sql)
		logger.debug(f"{self.LOG_TAG} Added 'logical_ddl.shadow_table' to publication 'pub_{node_name}'.")

		logger.debug(f"{self.LOG_TAG} Master '{node_name}' setup with ddl={False} finished successfully.")

	def setup_replica(self, node_name: str, master_node_name: str, ddl: bool, cascade: bool) -> None:
		logger.debug(f"{self.LOG_TAG} Settuing up replica '{node_name} with master '{master_node_name}'.")
		replica = self.config.get_node_by_name(node_name)

		set_subscriber_sql = f"""
			INSERT INTO logical_ddl.settings (publish, source)
			VALUES (false, '{master_node_name}')
		"""
		self._execute(node_name, set_subscriber_sql)
		logger.debug(f"{self.LOG_TAG} Node '{node_name}' is set as subscriber for source='{master_node_name}'.")


		set_replication_role_sql = """
			SET session_replication_role = 'replica';
		"""
		self._execute(node_name, set_replication_role_sql)
		logger.debug(f"{self.LOG_TAG} Set session_replication_role to 'replica' on '{node_name}'.")

		self.create_schema(node_name, replica.replication_schema)
		self.create_table(node_name, replica.replication_schema, replica.replication_table)

		subscription_name = f"sub_{node_name}"
		publication_name = f"pub_{master_node_name}"
		self.create_subscription(node_name, master_node_name, subscription_name, publication_name)

		if cascade:
			cascade_publication_name = f"pub_{node_name}"
			self.create_publication(node_name, cascade_publication_name, replica.replication_schema, False)

		logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with ddl={False} finish successfully.")



	def cleanup_cluster(self) -> None:
		logger.debug(f"{self.LOG_TAG} Cleaning up cluster for logical_ddl extension...")

		super().cleanup_cluster()

		for node in self.config.nodes:
			node_name = node.name
			try:
				drop_extension_sql = "DROP EXTENSION IF EXISTS logical_ddl CASCADE";
				self._execute(node_name, drop_extension_sql)
				logger.debug(f"{self.LOG_TAG} Deleted extension logical_ddl on '{node_name}'.")
			except Exception as e:
				logger.error(f"{self.LOG_TAG} Failed to drop extension logical_ddl on '{node_name}': {e}")

			try:
				create_extension_sql = "CREATE EXTENSION logical_ddl;";
				self._execute(node_name, create_extension_sql)
				logger.debug(f"{self.LOG_TAG} Create extension logical_ddl on '{node_name}'.")
			except Exception as e:
				logger.error(f"{self.LOG_TAG} Failed to create extension logical_ddl on '{node_name}': {e}")

		logger.debug(f"{self.LOG_TAG} Cleanup for logical_ddl extension completed on all nodes.")

	def create_table(self, node_name: str, schema_name: str, table_name: str,
					 columns_def: dict = None) -> None:
		super().create_table(node_name, schema_name, table_name, columns_def)
		self.add_table_in_extension(node_name=node_name, schema_name=schema_name, table_name=table_name)
		self.add_table_in_replication(node_name=node_name, schema_name=schema_name, table_name=table_name)

	def drop_table(self, node_name: str, schema_name: str, table_name: str) -> None:
		self.drop_table_from_extension(node_name=node_name, schema_name=schema_name, table_name=table_name)
		self.drop_table_from_replication(node_name=node_name, schema_name=schema_name, table_name=table_name)
		super().drop_table(node_name=node_name,schema_name=schema_name, table_name=table_name)

	def add_table_in_extension(self, node_name, schema_name, table_name):
		node = self.config.get_node_by_name(node_name)
		tablelist = "publish_tablelist" if node.role == "master" else "subscribe_tablelist"

		if node.role == "master":
			register_table_sql = f"""
			INSERT INTO logical_ddl.{tablelist} (relid)
			VALUES ('{schema_name}.{table_name}'::regclass);
			"""
		else:
			register_table_sql = f"""
			INSERT INTO logical_ddl.{tablelist} (source, relid)
			VALUES ('master', '{schema_name}.{table_name}'::regclass);
			"""
		self._execute(node_name, register_table_sql)
		logger.debug(f"{self.LOG_TAG} Record for table '{schema_name}.{table_name}' register in logical_ddl.{tablelist} on '{node_name}'.")

	def drop_table_from_extension(self, node_name, schema_name, table_name):
		node = self.config.get_node_by_name(node_name)
		tablelist = "publish_tablelist" if node.role == "master" else "subscribe_tablelist"

		remove_from_extension_sql = f"""
		DELETE FROM logical_ddl.{tablelist}
		WHERE relid = '{schema_name}.{table_name}'::regclass;
		"""
		self._execute(node_name, remove_from_extension_sql)
		logger.debug(f"{self.LOG_TAG} Record for table '{schema_name}.{table_name}' removed from logical_ddl.{tablelist} on '{node_name}'.")

	def add_table_in_replication(self, node_name, schema_name, table_name):
		node = self.config.get_node_by_name(node_name)
		if(node.role == "master"):
			add_table_to_publication_sql = f"""
			ALTER PUBLICATION pub_{node_name} ADD TABLE {schema_name}.{table_name};
			"""
			self._execute(node_name=node_name, sql = add_table_to_publication_sql)
			logger.debug(f"{self.LOG_TAG} Added '{schema_name}.{table_name}' to publication 'pub_{node_name}'.")
		else:
			check_subscription_sql = f"""
			SELECT 1
			FROM pg_subscription
			WHERE subname = 'sub_{node_name}';
			"""
			result = self._execute(node_name=node_name, sql=check_subscription_sql, fetch=True)
			if result:
				refresh_subscription_sql = f"""
				ALTER SUBSCRIPTION sub_{node_name} REFRESH PUBLICATION;
				"""
				self._execute(node_name=node_name, sql=refresh_subscription_sql, autocommit=True)
				logger.debug(f"{self.LOG_TAG} Refreshed subscription 'sub_{node_name}' after adding '{schema_name}.{table_name}' to publication.")
			else:
		   		logger.warning(f"{self.LOG_TAG} Subscription 'sub_{node_name}' does not exist. Skipping REFRESH PUBLICATION.")

	def drop_table_from_replication(self, node_name, schema_name, table_name):
		node = self.config.get_node_by_name(node_name)
		if(node.role == "master"):
			drop_pub_sql = f"""
				ALTER PUBLICATION pub_{node_name}
				DROP TABLE IF EXISTS {schema_name}.{table_name};
			"""
			self._execute(node_name=node_name, sql = drop_pub_sql)
			logger.debug(f"{self.LOG_TAG} Dropped '{schema_name}.{table_name}' from publication 'pub_{node_name}'.")
		else:
			refresh_subscription_sql = f"""
			ALTER SUBSCRIPTION sub_{node_name} REFRESH PUBLICATION;
			"""
			self._execute(node_name=node_name, sql=refresh_subscription_sql)
			logger.debug(f"{self.LOG_TAG} Refreshed subscription 'sub_{node_name}' after adding '{schema_name}.{table_name}' to publication.")