import click
from build import build_postgresql
from cluster import init_clusters, start_clusters, stop_clusters
from master import setup_master
from replica1 import setup_replica1
from replica2 import setup_replica2
from tests import run_tests

@click.group()
def cli():
    """CLI-приложение для автоматизации сборки и тестирования PostgreSQL."""
    pass

@cli.command()
def build():
    """Сборка и установка PostgreSQL."""
    build_postgresql()

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
    """Выполнение тестовых сценариев."""
    run_tests()

if __name__ == '__main__':
    cli()
