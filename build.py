import subprocess
import sys
import os
import click
import json

def build_postgresql():
    """Функция для сборки и установки PostgreSQL."""
    try:
        with open('config.json') as f:
            config = json.load(f)
        pg_source_dir = config['pg_source_dir']
        if not os.path.exists(pg_source_dir):
            click.echo(f"Путь к исходникам PostgreSQL не найден: {pg_source_dir}")
            sys.exit(1)

        os.chdir(pg_source_dir)
        click.echo("Очистка предыдущей сборки...")
        subprocess.run(['make', 'clean'], check=True)
        click.echo("Сборка PostgreSQL...")
        subprocess.run(['make'], check=True)
        click.echo("Установка PostgreSQL...")
        subprocess.run(['make', 'install'], check=True)
        click.echo("PostgreSQL успешно собран и установлен.")
    except subprocess.CalledProcessError as e:
        click.echo(f"Ошибка при выполнении команды: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Ошибка: {e}")
        sys.exit(1)
