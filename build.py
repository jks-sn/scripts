#build.py
import subprocess
import sys
import os
import click
import json

def build_postgresql(clean):
    """Функция для сборки и установки PostgreSQL."""
    try:
        with open('config.json') as f:
            config = json.load(f)
        pg_source_dir = config['pg_source_dir']
        if not os.path.exists(pg_source_dir):
            click.echo(f"Путь к исходникам PostgreSQL не найден: {pg_source_dir}")
            sys.exit(1)

        os.chdir(pg_source_dir)

        if clean:
            click.echo("Очистка предыдущей сборки...")
            subprocess.run(['make', 'clean'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        click.echo("Сборка PostgreSQL...")
        with open('build.log', 'a') as log_file:
            subprocess.run(['make'], check=True, stdout=log_file, stderr=subprocess.STDOUT)

        click.echo("Установка PostgreSQL...")
        with open('build.log', 'a') as log_file:
            subprocess.run(['make', 'install'], check=True, stdout=log_file, stderr=subprocess.STDOUT)

        click.echo("PostgreSQL успешно собран и установлен.")
    except subprocess.CalledProcessError as e:
        click.echo(f"Ошибка при выполнении команды: {e}")
        click.echo("Подробности ошибки смотрите в 'build.log'")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Ошибка: {e}")
        sys.exit(1)
