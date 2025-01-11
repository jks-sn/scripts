#commands/replica1.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from utils.config_loader import load_config
from utils.log_handler import logger


def setup_replica1(implementation: str, ddl: bool = False, cascade : bool = False):
    """
    Sets up Replica 1.

    :param implementation: Type of DDL implementation (ddl_patch, vanilla).
    :param ddl: If True, enable DDL replication in the publication.
    :param cascading_replication: If True, enable cascading replication.
    """
    try:
        config = load_config()

        replica1_server = next(c for c in config.clusters if c.name == "replica1")
        master_server = next(c for c in config.clusters if c.name == "master")

        replica1 = get_ddl_implementation(
            db_type="postgresql",
            implementation_type=implementation,
            config=config)

        master_subscription_info = (
            f"host={master_server.conn_params.host} "
            f"port={master_server.conn_params.port} "
            f"dbname={master_server.conn_params.dbname} "
            f"user={master_server.conn_params.user}"
        )
        server_name = replica1_server.name

        logger.debug("Setting up Replica 1...")

        try:
            logger.debug(f"Deploying schema and table on Replica1: {server_name}...")
            replica1.create_schema(server_name, replica1_server.replication_schema)
            replica1.create_table(server_name, replica1_server.replication_schema, replica1_server.replication_table)
            logger.debug(f"Schema and table deployed on {server_name}.")
        except Exception as e:
            logger.error(f"Error creating schema or table on {server_name}: {e}")
            sys.exit(1)

        try:
            logger.debug(f"Creating subscription to Master server on Replica 1: {server_name}...")
            replica1.create_subscription(
                cluster_name=server_name,
                subscription_name=f"sub_{server_name}",
                connection_info=master_subscription_info,
                publication_name=f"pub_{master_server.name}"
            )
            logger.debug(f"Subscription to Master created on {server_name}.")
        except Exception as e:
            logger.error(f"Error creating subscription on {server_name}: {e}")
            sys.exit(1)

        if(cascade):
            try:
                logger.debug(f"Creating publication on Replica 1: {server_name}...")
                replica1.create_publication(
                    cluster_name=server_name,
                    publication_name=f"pub_{server_name}",
                    schema_name=replica1_server.replication_schema,
                    ddl=ddl
                )
                logger.debug(f"Publication created on {server_name}.")
            except Exception as e:
                logger.error(f"Error creating publication on {server_name}: {e}")
                sys.exit(1)

        logger.debug("Replica 1 setup successfully.")

    except Exception as e:
        logger.error(f"Error setting up Replica 1: {e}")
        sys.exit(1)

@click.command(name="replica1")
@click.option('--ddl', is_flag=True, help='Enable DDL replication in the publication')
@click.option('--cascading-replication', is_flag=True, help='Enable cascading replication')
@click.pass_context
def setup_replica1_cmd(ctx, ddl: bool, cascading_replication: bool):
    """
    CLI command: Sets up Replica 1.
    """
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    setup_replica1(implementation=implementation, ddl=ddl, cascading_replication=cascading_replication)

