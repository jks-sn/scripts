#commands/replica1.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from utils.config_loader import load_config
from utils.log_handler import logger


def setup_replica1(implementation: str, ddl: bool = False, cascading_replication : bool = False):
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

        ddl_replica1 = get_ddl_implementation(
            db_type="postgresql",
            implementation_type=implementation,
            conn_params=replica1_server.conn_params.model_dump(),
            server_name=replica1_server.name)

        master_subscription_info = (
            f"host={master_server.conn_params.host} "
            f"port={master_server.conn_params.port} "
            f"dbname={master_server.conn_params.dbname} "
            f"user={master_server.conn_params.user}"
        )

        logger.debug("Setting up Replica 1...")

        try:
            logger.debug(f"Deploying schema and table on Replica1: {replica1_server.name}...")
            ddl_replica1.create_schema(replica1_server.replication_schema)
            ddl_replica1.create_table(replica1_server.replication_schema, replica1_server.replication_table)
            logger.debug(f"Schema and table deployed on {replica1_server.name}.")
        except Exception as e:
            logger.error(f"Error creating schema or table on {replica1_server.name}: {e}")
            sys.exit(1)

        try:
            logger.debug(f"Creating subscription to Master server on Replica 1: {replica1_server.name}...")
            ddl_replica1.create_subscription(
                subscription_name=f"sub_{replica1_server.name}",
                connection_info=master_subscription_info,
                publication_name=f"pub_{master_server.name}"
            )
            logger.debug(f"Subscription to Master created on {replica1_server.name}.")
        except Exception as e:
            logger.error(f"Error creating subscription on {replica1_server.name}: {e}")
            sys.exit(1)

        if(cascading_replication):
            try:
                logger.debug(f"Creating publication on Replica 1: {replica1_server.name}...")
                ddl_replica1.create_publication(
                    publication_name=f"pub_{replica1_server.name}",
                    schema_name=replica1_server.replication_schema,
                    ddl=ddl
                )
                logger.debug(f"Publication created on {replica1_server.name}.")
            except Exception as e:
                logger.error(f"Error creating publication on {replica1_server.name}: {e}")
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

