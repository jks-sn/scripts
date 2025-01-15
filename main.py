#main.py

import subprocess
import click
from commands.build import build_postgresql_cmd
from commands.cluster import (
    init_cluster_cmd,
    start_cluster_cmd,
    status_cluster_cmd,
    stop_cluster_cmd
)

from commands.master import setup_master_cmd
from commands.replica1 import setup_replica1_cmd
from commands.replica2 import setup_replica2_cmd
from commands.replication import setup_replication_cmd
from commands.clean_replication import clean_replication_cmd
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from tests.tests import tests_cmd

@click.group()
def cli():
    """CLI tool for automating PostgreSQL build, setup, and testing."""
    pass

cli.add_command(build_postgresql_cmd)

cli.add_command(init_cluster_cmd)
cli.add_command(start_cluster_cmd)
cli.add_command(status_cluster_cmd)
cli.add_command(stop_cluster_cmd)

cli.add_command(setup_replication_cmd)
cli.add_command(clean_replication_cmd)

cli.add_command(setup_master_cmd)
cli.add_command(setup_replica1_cmd)
cli.add_command(setup_replica2_cmd)

cli.add_command(tests_cmd)


@cli.command(name='full')
@click.option('--tags', '-t', multiple=True, help="Markers (tags) to run (e.g. ddl, cascade_ddl)")
def full_cmd(tags):
    """
    A full pipeline command:
      1) build PostgreSQL,
      2) init clusters,
      3) start clusters,
      4) run tests (pytest) with the specified markers/tags
    """

    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)

    ddl_replication.build_source(clean=False)

    ddl_replication.init_cluster()

    ddl_replication.start_cluster()

    from tests.tests import run_tests
    run_tests(tags=tags)

    ddl_replication.stop_cluster()

if __name__ == '__main__':
    cli()
