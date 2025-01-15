#commands/replica2.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from utils.log_handler import logger

@click.command()
@click.pass_context
def setup_replica2():
    """
    Sets up Replica 2.

    :param implementation: Type of DDL implementation (ddl_patch, vanilla).
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
    ddl_replication.setup_replica("replica2", "replica1", False, False)

@click.command(name="replica2")
@click.pass_context
def setup_replica2_cmd():
    """
    CLI command: Sets up Replica 2.
    """
    setup_replica2()
