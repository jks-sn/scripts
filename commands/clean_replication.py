#commands/clean_replication.py

import json
import click
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from utils.log_handler import logger

def clean_replication():
	"""
	Performs a full cleanup of all subscriptions, publications, replication slots,
	and replication schemas on all servers.
	"""
	config = load_config()
	ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
	ddl_replication.cleanup_cluster()

	logger.debug("Replication cleanup completed on all clusters.")

@click.command(name='clean')
def clean_replication_cmd():
	"""
	CLI command: Cleans up all subscriptions, publications, replication slots,
	and replication schemas on all servers.
	"""
	clean_replication()