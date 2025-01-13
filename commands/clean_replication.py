#commands/clean_replication.py

import json
import click
from utils.execute import execute_sql
from utils.config_loader import load_config
from utils.log_handler import logger

def clean_replication():
	"""
	Performs a full cleanup of all subscriptions, publications, replication slots,
	and replication schemas on all servers.
	"""
	config = load_config()
	clusters = config.clusters

	for cluster in clusters:
		server_name = cluster.name
		conn_params = cluster.conn_params.model_dump()  # pydantic -> dict
		subs = execute_sql(conn_params, "SELECT subname FROM pg_subscription;", server_name, fetch=True)
		for (subname,) in subs:
			drop_sub_sql = f"DROP SUBSCRIPTION {subname};"
			try:
				execute_sql(conn_params, drop_sub_sql, server_name, autocommit=True)
				logger.debug(f"Dropped subscription {subname} on {server_name}")
			except Exception as e:
				logger.error(f"Failed to drop subscription {subname} on {server_name}: {e}")


	for cluster in clusters:
		server_name = cluster.name
		conn_params = cluster.conn_params.model_dump()
		pubs = execute_sql(conn_params, "SELECT pubname FROM pg_publication;", server_name, fetch=True)
		for (pubname,) in pubs:
			drop_pub_sql = f"DROP PUBLICATION IF EXISTS {pubname};"
			execute_sql(conn_params, drop_pub_sql, server_name, autocommit=True)
			logger.debug(f"Dropped publication {pubname} on {server_name}")

	for cluster in clusters:
		server_name = cluster.name
		conn_params = cluster.conn_params.model_dump()
		schema_name = cluster.replication_schema
		drop_schema_sql = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
		execute_sql(conn_params, drop_schema_sql, server_name, autocommit=True)
		logger.debug(f"Dropped schema {schema_name} on {server_name}")

	logger.debug("Replication cleanup completed on all clusters.")
@click.command(name='clean')
def clean_replication_cmd():
	"""
	CLI command: Cleans up all subscriptions, publications, replication slots,
	and replication schemas on all servers.
	"""
	clean_replication()