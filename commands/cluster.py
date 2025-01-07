#commands/cluster.py

import subprocess
import os
import sys
import click
import json
from utils.execute import run_as_postgres
from utils.config_loader import load_config
from utils.log_handler import logger

def init_cluster():
    """
    Initializes PostgreSQL clusters by running initdb and writing config to postgresql.conf.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        for cluster in clusters:
            cluster_name = cluster.name
            data_dir = cluster.data_dir
            port = cluster.port

            if os.path.exists(data_dir):
                logger.debug(f"Removing previous data in cluster '{cluster_name}'...")
                run_as_postgres([f'rm -rf {data_dir}/*'])

            logger.debug(f"Initializing cluster '{cluster_name}'...")
            initdb_path = os.path.join(pg_bin_dir, 'initdb')
            run_as_postgres([initdb_path, '-D', data_dir])

            conf_path = os.path.join(data_dir, 'postgresql.conf')
            with open(conf_path, 'a') as conf_file:
                conf_file.write(f"\n# Settings for cluster {cluster_name}\n")
                conf_file.write(f"port = {port}\n")
                conf_file.write("wal_level = logical\n")
                conf_file.write("max_wal_senders = 10\n")
                conf_file.write("max_replication_slots = 10\n")
                conf_file.write("logging_collector = on\n")

        logger.debug("Clusters have been initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing clusters: {e}")
        sys.exit(1)

def start_cluster():
    """
    Starts all PostgreSQL clusters.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        for cluster in clusters:
            cluster_name = cluster.name
            data_dir = cluster.data_dir

            logger.debug(f"Starting cluster '{cluster_name}'...")
            pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'start'], drop_stdout=False)


        logger.debug("All clusters have been started successfully.")
    except Exception as e:
        logger.error(f"Error starting clusters: {e}")
        sys.exit(1)

def status_cluster():
    """
    Checks the status of all PostgreSQL clusters.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        for cluster in clusters:
            cluster_name = cluster.name
            data_dir = cluster.data_dir

            logger.debug(f"Checking status for cluster '{cluster_name}'...")
            pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'status'], drop_stdout=False)

        logger.debug("Status check completed for all clusters.")
    except Exception as e:
        logger.error(f"Error checking status of clusters: {e}")
        sys.exit(1)

def stop_cluster():
    """
    Stops all PostgreSQL clusters using pg_ctl.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        for cluster in clusters:
            cluster_name = cluster.name
            data_dir = cluster.data_dir

            logger.debug(f"Stopping cluster '{cluster_name}'...")
            pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
            run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop'], drop_stdout=False)

        logger.debug("All clusters have been stopped successfully.")
    except Exception as e:
        logger.error(f"Error stopping clusters: {e}")
        sys.exit(1)

def start_server(cluster_name):
    """
    Starts a specific PostgreSQL cluster by name.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == cluster_name), None)
        if not cluster:
            logger.debug(f"Cluster '{cluster_name}' not found.")
            return

        data_dir = cluster.data_dir
        logger.debug(f"Запуск кластера '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, '-l', os.path.join(data_dir, 'logfile'), 'start'])

        logger.debug(f"Cluster '{cluster_name}' has been started successfully.")
    except Exception as e:
        logger.error(f"Error starting cluster '{cluster_name}': {e}")
        sys.exit(1)

def stop_server(cluster_name):
    """
    Stops a specific PostgreSQL cluster by name.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == cluster_name), None)
        if not cluster:
            logger.debug(f"Cluster '{cluster_name}' not found.")
            return

        data_dir = cluster.data_dir
        logger.debug(f"Stopping cluster '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop', '-m', 'fast'])

        logger.debug(f"Cluster '{cluster_name}' has been stopped successfully.")
    except Exception as e:
        logger.error(f"Error stopping cluster '{cluster_name}': {e}")
        sys.exit(1)

@click.command(name='init')
def init_cluster_cmd():
    """CLI command: Initialize cluster."""
    init_cluster()

@click.command(name='start')
def start_cluster_cmd():
    """CLI command: Start cluster."""
    start_cluster()

@click.command(name='status')
def status_cluster_cmd():
    """CLI command: Check status of the cluster."""
    status_cluster()

@click.command(name='stop')
def stop_cluster_cmd():
    """CLI command: Stop cluster."""
    stop_cluster()