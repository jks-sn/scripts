#main.py

import os
import sys
import unittest
import click
from commands.build import build_postgresql
from commands.cluster import init_clusters, start_clusters, stop_clusters
from commands.master import setup_master
from commands.replica1 import setup_replica1
from commands.replica2 import setup_replica2
from commands.replication import setup_replication
from commands.clean_replication import clean_replication

@click.group()
def cli():
    """CLI-приложение для автоматизации сборки, настройки и тестирования PostgreSQL."""
    pass

@cli.command()
def clean():
    """Очистка подписок, публикаций и схем на всех серверах."""
    clean_replication()


@cli.command()
@click.option('--clean', is_flag=True, help='Выполнить make clean перед сборкой')
def build(clean):
    """Сборка и установка PostgreSQL."""
    build_postgresql(clean)

@cli.command()
def init():
    """Инициализация кластеров."""
    init_clusters()

@cli.command()
def start():
    """Запуск кластеров."""
    start_clusters()

@cli.command()
def stop():
    """Остановка кластеров."""
    stop_clusters()

@cli.command()
@click.option('--ddl', is_flag=True, help='Включить DDL репликацию в публикации')
def master(ddl):
    """Настройка Master."""
    setup_master(ddl=ddl)

@cli.command()
@click.option('--ddl', is_flag=True, help='Включить DDL репликацию в публикации')
def replica1(ddl):
    """Настройка Replica 1."""
    setup_replica1(ddl=ddl)

@cli.command()
def replica2():
    """Настройка Replica 2."""
    setup_replica2()

@cli.command()
@click.option('--ddl', is_flag=True, help='Включить DDL репликацию в публикации')
def create(ddl):
    """Полная настройка репликации."""
    setup_replication(ddl=ddl)

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
def tests(tags):
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
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    cli()
