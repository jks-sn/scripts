#commands/cluster.py

import subprocess
import os
import sys
import click
import json
from utils.execute import run_as_postgres
from utils.config_loader import load_config
from utils.log_handler import logger

def init_clusters():
    """Функция для инициализации кластеров."""
    try:
        config = load_config()
        pg_bin_dir = config['pg_bin_dir']
        clusters = config['clusters']

        for cluster in clusters:
            data_dir = cluster['data_dir']
            port = cluster['port']
            cluster_name = cluster['name']

            if os.path.exists(data_dir):
                logger.debug(f"Удаление предыдущих данных кластера '{cluster_name}'...")
                run_as_postgres([f'rm -rf {data_dir}/*'])

            logger.debug(f"Инициализация кластера '{cluster_name}'...")
            initdb_path = os.path.join(pg_bin_dir, 'initdb')
            run_as_postgres([initdb_path, '-D', data_dir])

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

    except Exception as e:
        logger.error(f"Ошибка при инициализации кластеров: {e}")
        sys.exit(1)

def start_clusters():
    """Функция для запуска кластеров."""
    try:
        config = load_config()
        pg_bin_dir = config['pg_bin_dir']
        clusters = config['clusters']

        for cluster in clusters:
            data_dir = cluster['data_dir']
            cluster_name = cluster['name']

            logger.debug(f"Запуск кластера '{cluster_name}'...")
            pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'start'])

        logger.debug("Все кластеры успешно запущены.")
    except Exception as e:
        logger.error(f"Ошибка при запуске кластеров: {e}")
        sys.exit(1)

def stop_clusters():
    """Функция для остановки кластеров."""
    try:
        config = load_config()  # Загружаем конфигурацию
        pg_bin_dir = config['pg_bin_dir']
        clusters = config['clusters']

        for cluster in clusters:
            data_dir = cluster['data_dir']
            cluster_name = cluster['name']

            logger.debug(f"Остановка кластера '{cluster_name}'...")
            pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop'])

        logger.debug("Все кластеры успешно остановлены.")
    except Exception as e:
        logger.error(f"Ошибка при остановке кластеров: {e}")
        sys.exit(1)

def start_cluster(cluster_name):
    """Функция для запуска конкретного кластера по имени."""
    try:
        config = load_config()
        pg_bin_dir = config['pg_bin_dir']
        clusters = config['clusters']

        cluster = next((c for c in clusters if c['name'] == cluster_name), None)
        if not cluster:
            logger.debug(f"Кластер '{cluster_name}' не найден.")
            return

        data_dir = cluster['data_dir']
        logger.debug(f"Запуск кластера '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, '-l', os.path.join(data_dir, 'logfile'), 'start'])

        logger.debug(f"Кластер '{cluster_name}' успешно запущен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске кластера '{cluster_name}': {e}")
        sys.exit(1)

def stop_cluster(cluster_name):
    """Функция для остановки конкретного кластера по имени."""
    try:
        config = load_config()
        pg_bin_dir = config['pg_bin_dir']
        clusters = config['clusters']

        cluster = next((c for c in clusters if c['name'] == cluster_name), None)
        if not cluster:
            logger.debug(f"Кластер '{cluster_name}' не найден.")
            return

        data_dir = cluster['data_dir']
        logger.debug(f"Остановка кластера '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop', '-m', 'fast'])

        logger.debug(f"Кластер '{cluster_name}' успешно остановлен.")
    except Exception as e:
        logger.error(f"Ошибка при остановке кластера '{cluster_name}': {e}")
        sys.exit(1)