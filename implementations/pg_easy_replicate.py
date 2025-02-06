# implementations/pg_easy_replicate.py

import os
import time
from implementations.base_ddl import BaseDDL
from utils.execute import execute_sql
from utils.log_handler import logger
import subprocess
import sys

class PG_Easy_Replicate(BaseDDL):

	LOG_TAG = "[PG_EASY_REPLICATE]"

	def __init__(self, config):
		super().__init__(config)

	def get_cli_env(self) -> dict:
		env = os.environ.copy()
		env["SOURCE_DB_URL"] = "postgres://replication:replication@localhost:5432/postgres"
		env["TARGET_DB_URL"] = "postgres://replication:replication@localhost:5433/postgres"
		env["PATH"] = env.get("PATH", "") + f":{self.config.pg_bin_dir}"
		env["GEM_HOME"] = "/home/jks/.local/share/gem/ruby"
		env["GEM_PATH"] = (
			"/home/jks/.local/share/gem/ruby:"
			"/usr/share/gems:"
			"/usr/local/share/gems:"
			"/lib64/gems/ruby"
		)
		return env

	def init_cluster(self) -> None:
		logger.debug(f"{self.LOG_TAG} Init cluster starting with pg_easy_replicate cli build...")

		super().init_cluster()

		logger.debug(f"{self.LOG_TAG} Starting all cluster nodes for bootstrap.")
		self.start_cluster()

		time.sleep(3)

		cli_env = self.get_cli_env()
		logger.debug(f"{self.LOG_TAG} Running bootstrap command with environment: GEM_HOME={cli_env.get('GEM_HOME')}, GEM_PATH={cli_env.get('GEM_PATH')}")
		bootstrap_cmd = ["/home/jks/bin/pg_easy_replicate", "bootstrap", "--group-name=repl1"]
		logger.debug(f"{self.LOG_TAG} Running bootstrap command: {' '.join(bootstrap_cmd)}")
		try:
			subprocess.run(bootstrap_cmd, check=True, env=cli_env)
			logger.debug(f"{self.LOG_TAG} Bootstrap completed successfully.")
		except subprocess.CalledProcessError as e:
			logger.error(f"{self.LOG_TAG} Bootstrap command failed: {e}")
			sys.exit(1)
		logger.debug(f"{self.LOG_TAG} Cluster initialization completed successfully.")


	def setup_master(self, node_name: str, ddl: bool) -> None:
		logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with pg_easy_replicate cli...")

		node = self.config.get_node_by_name(node_name)
		self.create_schema(node_name, node.replication_schema)
		self.create_table(node_name, node.replication_schema, node.replication_table)

		logger.debug(f"{self.LOG_TAG} Master '{node_name}' setup completed successfully.")

	def setup_replica(self, node_name: str, master_node_name: str, ddl: bool, cascade: bool) -> None:
		logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with pg_easy_replicate cli...")
		node = self.config.get_node_by_name(node_name)
		self.create_schema(node_name, node.replication_schema)
		self.create_table(node_name, node.replication_schema, node.replication_table)

		cli_env = self.get_cli_env()
		start_sync_cmd = [
			"/home/jks/bin/pg_easy_replicate",
			"start_sync",
			"--group_name=repl1",
			"--track-ddl",
			f"--schema_name={node.replication_schema}"
		]
		logger.debug(f"{self.LOG_TAG} Running start_sync command: {' '.join(start_sync_cmd)}")
		try:
			subprocess.run(start_sync_cmd, check=True, env=cli_env)
			logger.debug(f"{self.LOG_TAG} Replica sync started successfully.")
		except subprocess.CalledProcessError as e:
			logger.error(f"{self.LOG_TAG} start_sync command failed: {e}")
			sys.exit(1)
		logger.debug(f"{self.LOG_TAG} Replica '{node_name}' setup completed successfully.")

	def cleanup_cluster(self) -> None:
		logger.debug(f"{self.LOG_TAG} Cleaning up replication via pg_easy_replicate cli...")

		cli_env = self.get_cli_env()
		stop_sync_cmd = ["/home/jks/bin/pg_easy_replicate", "stop_sync", "-g", "repl1"]
		logger.debug(f"{self.LOG_TAG} Running stop_sync command: {' '.join(stop_sync_cmd)}")
		try:
			subprocess.run(stop_sync_cmd, check=True, env=cli_env)
			logger.debug(f"{self.LOG_TAG} stop_sync completed successfully.")
		except subprocess.CalledProcessError as e:
			logger.error(f"{self.LOG_TAG} stop_sync command failed: {e}")
			sys.exit(1)

		cleanup_cmd = ["/home/jks/bin/pg_easy_replicate", "cleanup", "-g", "repl1", "-e"]
		logger.debug(f"{self.LOG_TAG} Running cleanup command: {' '.join(cleanup_cmd)}")
		try:
			subprocess.run(cleanup_cmd, check=True, env=cli_env)
			logger.debug(f"{self.LOG_TAG} cleanup completed successfully.")
		except subprocess.CalledProcessError as e:
			logger.error(f"{self.LOG_TAG} cleanup command failed: {e}")
			sys.exit(1)

		for node in self.config.nodes:
			node_name = node.name
			schema_name = node.replication_schema
			try:
				self.drop_schema(node_name, schema_name)
				logger.debug(f"{self.LOG_TAG} Dropped schema '{schema_name}' on node '{node_name}'.")
			except Exception as e:
				logger.error(f"{self.LOG_TAG} Failed to drop schema '{schema_name}' on node '{node_name}': {e}")

		logger.debug(f"{self.LOG_TAG} Cluster cleanup completed successfully.")