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
from tests.tests import tests_cmd

@click.group()
@click.option(
    '--implementation',
    type=click.Choice(['ddl_patch', 'vanilla'], case_sensitive=False),
    default='vanilla',
    help='Type of DDL implementation (ddl_patch, vanilla)'
)
@click.pass_context
def cli(ctx, implementation):
    """CLI tool for automating PostgreSQL build, setup, and testing."""
    ctx.ensure_object(dict)
    ctx.obj['IMPLEMENTATION'] = implementation

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
@click.pass_context
def full_cmd(ctx, tags):
    """
    A full pipeline command:
      1) build PostgreSQL,
      2) init clusters,
      3) start clusters,
      4) run tests (pytest) with the specified markers/tags
    """
    implementation = ctx.obj['IMPLEMENTATION']

    from commands.build import build_postgresql
    build_postgresql(clean=True, implementation=implementation)

    from commands.cluster import init_cluster
    init_cluster(implementation=implementation)


    from commands.cluster import start_cluster
    start_cluster(implementation=implementation)

    from tests.tests import run_tests
    run_tests(implementation=implementation, tags=tags)


if __name__ == '__main__':
    cli(obj={})
