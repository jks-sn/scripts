#commands/master.py

import sys
import click
from factories.ddl_factory import get_ddl_implementation
from utils.config_loader import load_config
from utils.log_handler import logger

def setup_master(implementation: str, ddl=False):
    """
    Sets up the Master server.

    :param implementation: Type of DDL implementation.
    :param ddl: If True, enable DDL replication in the publication.
    """
    try:
        config = load_config()

        master_server = next(c for c in config.clusters if c.name == "master")

        ddl_master = get_ddl_implementation(
            db_type="postgresql",
            implementation_type=implementation,
            config=config)

        server_name = master_server.name
        try:
            logger.debug(f"Deploying schema and table on {server_name}...")
            ddl_master.create_schema(server_name, master_server.replication_schema)
            ddl_master.create_table(server_name, master_server.replication_schema, master_server.replication_table)
            logger.debug(f"Schema and table deployed on {server_name}.")
        except Exception as e:
            logger.error(f"Error creating schema or table on {server_name}: {e}")
            sys.exit(1)

        try:
            logger.debug(f"Creating publication on {server_name}...")
            ddl_master.create_publication(
                server_name,
                publication_name=f"pub_{server_name}",
                schema_name=master_server.replication_schema,
                ddl=ddl)
        except Exception as e:
            logger.error(f"Error creating publication on {server_name}: {e}")
            sys.exit(1)

        logger.debug("Master setup successfully completed.")
    except Exception as e:
        logger.error(f"Error setting up master: {e}")
        sys.exit(1)

@click.command(name="master")
@click.option('--ddl', is_flag=True, help='Enable DDL replication in the publication')
def setup_master_cmd(ctx, ddl: bool):
    """Sets up the Master server."""
    implementation = ctx.obj.get('IMPLEMENTATION', 'vanilla')
    setup_master(implementation=implementation, ddl=ddl)
