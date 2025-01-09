#commands/replication.py

import sys
import click
from commands.master import setup_master
from commands.replica1 import setup_replica1
from commands.replica2 import setup_replica2
from utils.log_handler import logger

def setup_replication(implementation: str, ddl: bool = False, cascade: bool = False):
    """
    Performs a full replication setup:
    1) Sets up the Master (optionally with DDL replication).
    2) Sets up Replica 1 (optionally with DDL replication and cascading replication).
    3) Sets up Replica 2.

    :param implementation: Type of DDL implementation.
    :param ddl: If True, enable DDL replication in the publication.
    :param cascade: If True, enable cascading replication.
    """
    try:
        logger.debug("Starting master setup...")
        setup_master(implementation=implementation, ddl=ddl)
        logger.debug("Master setup completed.")
    except Exception as e:
        logger.error(f"Error during master setup: {e}")
        raise

    try:
        logger.debug("Starting Replica 1 setup...")
        setup_replica1(implementation=implementation, ddl=ddl, cascade=cascade)
        logger.debug("Replica 1 setup completed.")
    except Exception as e:
        logger.error(f"Error during Replica 1 setup: {e}")
        raise

    if(cascade):
        try:
            logger.debug("Starting Replica 2 setup...")
            setup_replica2(implementation=implementation)
            logger.debug("Replica 2 setup completed.")
        except Exception as e:
            logger.error(f"Error during Replica 2 setup: {e}")
            raise

    logger.debug("Replication setup has been completed successfully.")
    click.echo("Replication setup has been completed successfully.")

@click.command(name="setup-replication")
@click.option('--ddl', is_flag=True, help='Enable DDL replication in the publication')
@click.option('--cascading-replication', is_flag=True, help='Enable cascading replication for Replica 1')
@click.pass_context
def setup_replication_cmd(ctx, ddl: bool, cascade: bool):
    """
    CLI command: Performs a full replication setup (Master, Replica1, and Replica2).
    """
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    try:
        setup_replication(
            implementation=implementation,
            ddl=ddl,
            cascade=cascade
        )
        click.secho("Full replication setup is complete.", fg='green')
    except Exception as e:
        click.secho(f"Failed to complete replication setup: {e}", fg='red')
        sys.exit(1)

