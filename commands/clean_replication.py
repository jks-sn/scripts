#commands/clean_replication.py

import json
import click
from utils.execute import execute_sql
from utils.config_loader import load_config
from utils.log_handler import logger

def clean_replication():
    """
    Performs a full cleanup of all subscriptions, publications, replication slots,
    and replication schemas on all servers.
    """
    try:
        config = load_config()
        clusters = config.clusters

        for cluster in clusters:
            server_name = cluster.name
            conn_params = cluster.conn_params.dict()
            schema_name = cluster.replication_schema

            logger.debug(f"Starting full replication cleanup on server '{server_name}'...")

            subscriptions = execute_sql(
                conn_params=conn_params,
                sql="SELECT subname FROM pg_subscription;",
                server_name=server_name,
                fetch=True
            )

            for sub in subscriptions:
                        sub_name = sub[0]
                        execute_sql(
                            conn_params=conn_params,
                            sql=f"DROP SUBSCRIPTION IF EXISTS {sub_name};",
                            server_name=server_name,
                            autocommit=True
                        )

            publications = execute_sql(
                conn_params=conn_params,
                sql="SELECT pubname FROM pg_publication;",
                server_name=server_name,
                fetch=True
            )
            for pub in publications:
                pub_name = pub[0]
                execute_sql(
                    conn_params=conn_params,
                    sql=f"DROP PUBLICATION IF EXISTS {pub_name};",
                    server_name=server_name
                )

            slots = execute_sql(
                conn_params=conn_params,
                sql="SELECT slot_name FROM pg_replication_slots WHERE plugin = 'pgoutput';",
                server_name=server_name,
                fetch=True
            )
            for slot in slots:
                slot_name = slot[0]
                execute_sql(
                    conn_params=conn_params,
                    sql=f"SELECT pg_drop_replication_slot('{slot_name}');",
                    server_name=server_name
                )


            execute_sql(
                conn_params=conn_params,
                sql=f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;",
                server_name=server_name
            )

            logger.debug(f"Replication cleanup completed on server '{server_name}'.")

        logger.debug("Replication cleanup completed on all clusters.")
    except Exception as e:
        logger.error(f"Error during replication cleanup: {e}")
        raise

@click.command(name='clean')
def clean_replication_cmd():
    """
    CLI command: Cleans up all subscriptions, publications, replication slots,
    and replication schemas on all servers.
    """
    clean_replication()