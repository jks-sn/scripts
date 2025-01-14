#commands/replica1.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from utils.log_handler import logger


def setup_replica1(implementation: str, ddl: bool = False, cascade : bool = False):
    """
    Sets up Replica 1.

    :param implementation: Type of DDL implementation (ddl_patch, vanilla).
    :param ddl: If True, enable DDL replication in the publication.
    :param cascading_replication: If True, enable cascading replication.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.setup_replica("replica1", "master", ddl, cascade)

@click.command(name="replica1")
@click.option('--ddl', is_flag=True, help='Enable DDL replication in the publication')
@click.option('--cascading-replication', is_flag=True, help='Enable cascading replication')
@click.pass_context
def setup_replica1_cmd(ctx, ddl: bool, cascading_replication: bool):
    """
    CLI command: Sets up Replica 1.
    """
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    setup_replica1(implementation=implementation, ddl=ddl, cascading_replication=cascading_replication)

