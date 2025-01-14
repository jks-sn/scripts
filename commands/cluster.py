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


def init_cluster(implementation: str = "vanilla"):
    """
    Init cluster
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.init_cluster()

def start_cluster(implementation: str = "vanilla"):
    """
    Starts all PostgreSQL clusters.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.start_cluster()

def status_cluster(implementation: str = "vanilla"):
    """
    Checks the status of all PostgreSQL clusters.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.status_cluster()

def stop_cluster():
    """
    Stops all PostgreSQL clusters using pg_ctl.
    """
    config = load_config()
    ddl_replication = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
    ddl_replication.stop_cluster()

def start_server(cluster_name):
    """
    Starts a specific PostgreSQL cluster by name.
    """
    try:
        config = load_config()
        pg_bin_dir = config.pg_bin_dir
        pg_cluster_dir = config.pg_cluster_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == cluster_name), None)
        if not cluster:
            logger.debug(f"Cluster '{cluster_name}' not found.")
            return

        data_dir = os.path.join(pg_cluster_dir, cluster_name)
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
        pg_cluster_dir = config.pg_cluster_dir
        clusters = config.clusters

        cluster = next((c for c in clusters if c.name == cluster_name), None)
        if not cluster:
            logger.debug(f"Cluster '{cluster_name}' not found.")
            return

        data_dir = os.path.join(pg_cluster_dir, cluster_name)
        logger.debug(f"Stopping server '{cluster_name}'...")
        pg_ctl_path = os.path.join(pg_bin_dir, 'pg_ctl')
        run_as_postgres([pg_ctl_path, '-D', data_dir, 'stop', '-m', 'fast'])

        logger.debug(f"Cluster '{cluster_name}' has been stopped successfully.")
    except Exception as e:
        logger.error(f"Error stopping cluster '{cluster_name}': {e}")
        sys.exit(1)

@click.command(name='init')
def init_cluster_cmd(ctx):
    """CLI command: Initialize cluster."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    init_cluster(implementation)

@click.command(name='start')
def start_cluster_cmd(ctx):
    """CLI command: Start cluster."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    start_cluster(implementation)

@click.command(name='status')
def status_cluster_cmd(ctx):
    """CLI command: Check status of the cluster."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    status_cluster(implementation)

@click.command(name='stop')
def stop_cluster_cmd(ctx):
    """CLI command: Stop cluster."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    stop_cluster(implementation)