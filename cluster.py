import subprocess
import os
import click
import json

def init_clusters():
    """Функция для инициализации кластеров."""
    with open('config.json') as f:
        config = json.load(f)

    pg_bin_dir = config['pg_bin_dir']
    clusters = config['clusters']
    for cluster in clusters:
        data_dir = cluster['data_dir']
        port = cluster['port']
        cluster_name = cluster['name']

        # Удаление предыдущих данных (если необходимо)
        if os.path.exists(data_dir):
            click.echo(f"Удаление предыдущих данных кластера '{cluster_name}'...")
            subprocess.run(['rm', '-rf', data_dir], check=True)

        click.echo(f"Инициализация кластера '{cluster_name}'...")
        initdb_path = os.path.join(pg_bin_dir, 'initdb')
        subprocess.run([initdb_path, '-D', data_dir], check=True)

        # Настройка параметров в postgresql.conf
        conf_path = os.path.join(data_dir, 'postgresql.conf')
        with open(conf_path, 'a') as conf_file:
            conf_file.write(f"\n# Настройки для кластера {cluster_name}\n")
            conf_file.write(f"port = {port}\n")
            conf_file.write("wal_level = logical\n")
            conf_file.write("max_wal_senders = 10\n")
            conf_file.write("max_replication_slots = 10\n")
            conf_file.write("logging_collector = on\n")
            # Добавьте дополнительные параметры по необходимости

        # Настройка pg_hba.conf для разрешения подключений
        # hba_path = os.path.join(data_dir, 'pg_hba.conf')
        # with open(hba_path, 'a') as hba_file:
        #     hba_file.write("\n# Разрешение подключений для репликации\n")
        #     hba_file.write("host replication all 127.0.0.1/32 trust\n")
        #     hba_file.write("host all all 127.0.0.1/32 trust\n")

def start_clusters():
    """Функция для запуска кластеров."""
    with open('config.json') as f:
        config = json.load(f)

    pg_bin_dir = config['pg_bin_dir']
    clusters = config['clusters']
    for cluster in clusters:
        data_dir = cluster['data_dir']
        cluster_name = cluster['name']

        click.echo(f"Запуск кластера '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        subprocess.run([pg_ctl_path, '-D', data_dir, 'start'], check=True)

def stop_clusters():
    """Функция для остановки кластеров."""
    with open('config.json') as f:
        config = json.load(f)

    pg_bin_dir = config['pg_bin_dir']
    clusters = config['clusters']
    for cluster in clusters:
        data_dir = cluster['data_dir']
        cluster_name = cluster['name']

        click.echo(f"Остановка кластера '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        subprocess.run([pg_ctl_path, '-D', data_dir, 'stop', '-m', 'fast'], check=True)
