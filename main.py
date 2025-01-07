#main.py

import os
import sys
import unittest
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
from utils.tesstHandler import TestHandler

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

def iterate_tests(suite):
    """Рекурсивно обходит все тесты в TestSuite."""
    for test in suite:
        if isinstance(test, unittest.TestCase):
            yield test
        elif isinstance(test, unittest.TestSuite):
            yield from iterate_tests(test)
        else:
            # Неожиданный тип объекта
            pass

@cli.command()
@click.option('--tags', '-t', multiple=True, help="Теги тестов для запуска (dml, ddl)")
def tests_cmd(tags):
    """Запуск тестов."""
    click.echo("Запуск тестов...")
    loader = unittest.TestLoader()
    tests = loader.discover('tests')

    if tags:
        # Фильтруем тесты по тегам
        filtered_tests = unittest.TestSuite()
        for test in iterate_tests(tests):
            test_method = getattr(test, test._testMethodName)
            test_tags = getattr(test_method, 'tags', [])
            if any(tag in test_tags for tag in tags):
                filtered_tests.addTest(test)
        tests = filtered_tests

    click.echo(f"Найдено тестов: {tests.countTestCases()}")
    runner = unittest.TextTestRunner(resultclass=TestHandler, verbosity=2)
    result = runner.run(tests)

    print("\nTest Summary:")
    print(f"Total tests run: {result.testsRun}")
    print(f"Successes: {len(result.successes)}")
    print(f"Failures: {len(result.failures_list)}")
    print(f"Errors: {len(result.errors_list)}")
    print(f"Skipped: {len(result.skipped_list)}")
    sys.exit(0 if result.wasSuccessful() else 1)

cli.add_command(tests_cmd)
if __name__ == '__main__':
    cli(obj={})
