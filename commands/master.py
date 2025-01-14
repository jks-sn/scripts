#commands/master.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from utils.log_handler import logger

def setup_master(implementation: str, ddl=False):
    """
    Sets up the Master server.

    :param implementation: Type of DDL implementation.
    :param ddl: If True, enable DDL replication in the publication.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.setup_master("master", ddl)

@click.command(name="master")
@click.option('--ddl', is_flag=True, help='Enable DDL replication in the publication')
def setup_master_cmd(ctx, ddl: bool):
    """Sets up the Master server."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    setup_master(implementation=implementation, ddl=ddl)
