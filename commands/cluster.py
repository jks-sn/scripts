#commands/cluster.py

import grp
import pwd
import subprocess
import os
import sys
import click
import json
from factories.ddl_factory import get_ddl_implementation
from utils.execute import run_as_postgres
from models.config import load_config
from utils.log_handler import logger


def init_cluster():
    """
    Init cluster
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
    ddl_replication.init_cluster()

def start_cluster():
    """
    Starts all PostgreSQL clusters.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
    ddl_replication.start_cluster()

def status_cluster():
    """
    Checks the status of all PostgreSQL clusters.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
    ddl_replication.status_cluster()

def stop_cluster():
    """
    Stops all PostgreSQL clusters using pg_ctl.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", config=config)
    ddl_replication.stop_cluster()

def start_server(server_name):
    """
    Starts a specific PostgreSQL cluster by name.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        pg_cluster_dir = config.pg_cluster_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == server_name), None)
        if not cluster:
            logger.debug(f"Cluster '{server_name}' not found.")
            return

        data_dir = os.path.join(pg_cluster_dir, server_name)
        logger.debug(f"Запуск кластера '{server_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, '-l', os.path.join(data_dir, 'logfile'), 'start'])

        logger.debug(f"Cluster '{server_name}' has been started successfully.")
    except Exception as e:
        logger.error(f"Error starting cluster '{server_name}': {e}")
        sys.exit(1)

def stop_server(server_name):
    """
    Stops a specific PostgreSQL cluster by name.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        pg_cluster_dir = config.pg_cluster_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == server_name), None)
        if not cluster:
            logger.debug(f"Cluster '{server_name}' not found.")
            return

        data_dir = os.path.join(pg_cluster_dir, server_name)
        logger.debug(f"Stopping server '{server_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop', '-m', 'fast'])

        logger.debug(f"Cluster '{server_name}' has been stopped successfully.")
    except Exception as e:
        logger.error(f"Error stopping cluster '{server_name}': {e}")
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