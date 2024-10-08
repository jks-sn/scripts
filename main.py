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
def master():
    """Настройка Master 1."""
    setup_master()

@cli.command()
def replica1():
    """Настройка Replica 1."""
    setup_replica1()

@cli.command()
def replica2():
    """Настройка Replica 2."""
    setup_replica2()

@cli.command()
def create():
    """Полная настройка репликации."""
    setup_replication()

@cli.command()
def tests():
    """Запуск тестов."""
    click.echo("Запуск тестов...")
    # Добавляем путь к директории tests в sys.path
    tests_dir = os.path.join(os.path.dirname(__file__), 'tests')
    sys.path.append(tests_dir)
    # Запускаем тесты
    unittest.main(module=None, argv=[''], exit=False)

if __name__ == '__main__':
    cli()
