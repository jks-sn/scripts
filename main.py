#main.py
import json
import click
from build import build_postgresql
from cluster import init_clusters, start_clusters, stop_clusters
from master import setup_master
from replica1 import setup_replica1
from replica2 import setup_replica2
from tests import run_tests
from db_utils import clean_replication_setup

@click.group()
def cli():
    """CLI-приложение для автоматизации сборки, настройки и тестирования PostgreSQL."""
    pass

@cli.command()
def clean():
    """Очистка подписок, публикаций и схем на всех серверах."""
    clean_replication_setup()


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
def test():
    """Выполнение тестов."""
    run_tests()

if __name__ == '__main__':
    cli()
