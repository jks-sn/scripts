#commands/replica2.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from utils.config_loader import load_config
from utils.log_handler import logger

@click.command()
@click.pass_context
def setup_replica2(implementation: str):
    """
    Sets up Replica 2.

    :param implementation: Type of DDL implementation (ddl_patch, vanilla).
    """
    try:
        config = load_config()

        replica1_server = next(c for c in config.clusters if c.name == "replica1")
        replica2_server = next(c for c in config.clusters if c.name == "replica2")

        ddl_replica2 = get_ddl_implementation(
            db_type="postgresql",
            implementation_type=implementation,
            config=config
        )

        server_name = replica2_server.name

        replica1_subscription_info = (
            f"host={replica1_server.conn_params.host} "
            f"port={replica1_server.conn_params.port} "
            f"dbname={replica1_server.conn_params.dbname} "
            f"user={replica1_server.conn_params.user}"
        )

        logger.debug("Setting up Replica 2...")

        try:
            logger.debug(f"Deploying schema and table on Replica 2: {server_name}...")
            ddl_replica2.create_schema(server_name, replica2_server.replication_schema)
            ddl_replica2.create_table(server_name, replica2_server.replication_schema, replica2_server.replication_table)
            logger.debug(f"Schema and table deployed on {server_name}.")
        except Exception as e:
            logger.error(f"Error creating schema or table on {server_name}: {e}")
            sys.exit(1)

        try:
            logger.debug(f"Creating subscription to Replica 1 on Replica 2: {server_name}...")
            ddl_replica2.create_subscription(
                cluster_name=server_name,
                subscription_name=f"sub_{server_name}",
                connection_info=replica1_subscription_info,
                publication_name=f"pub_{replica1_server.name}"
            )
            logger.debug(f"Subscription to Replica 1 created on {server_name}.")
        except Exception as e:
            logger.error(f"Error creating subscription on {server_name}: {e}")
            sys.exit(1)

        logger.debug("Replica 2 setup successfully completed.")

    except Exception as e:
        logger.error(f"Error setting up Replica 2: {e}")
        sys.exit(1)

@click.command(name="replica2")
@click.pass_context
def setup_replica2_cmd(ctx):
    """
    CLI command: Sets up Replica 2.
    """
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    setup_replica2(implementation=implementation)
