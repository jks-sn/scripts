#commands/build.py

import subprocess
import sys
import os
import click
from utils.config_loader import load_config
from utils.log_handler import logger

def build_postgresql(clean: bool = False,  implementation: str = "vanilla"):
    """
    Builds and installs PostgreSQL from source.

    :param clean: If True, runs 'make clean' before building.
    """
    try:
        config = load_config()
        pg_source_dir = config.pg_source_dir

        if not os.path.exists(pg_source_dir):
            click.echo(f"PostgreSQL source directory not found: {pg_source_dir}")
            sys.exit(1)

        if clean:
            logger.debug("Running 'make clean' before build...")
            result = subprocess.run(
                ['make', 'clean'],
                cwd=pg_source_dir,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                text=True
            )
            logger.debug(f"make clean output:\n{result.stdout}")

        logger.debug("Running './configure'...")
        result = subprocess.run(
            ['./configure'],
            cwd=pg_source_dir,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=True
            )
        logger.debug(f"'./configure' output:\n{result.stdout}")

        logger.debug("Running 'make'...")
        result = subprocess.run(
            ['make'],
            cwd=pg_source_dir,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            shell=True,
            text=True
        )
        logger.debug(f"'make' output:\n{result.stdout}")

        logger.debug("Running 'make install'...")
        result = subprocess.run(
            ['make', 'install'],
            cwd=pg_source_dir,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=True
        )
        logger.debug(f"'make install' output:\n{result.stdout}")

        logger.debug("PostgreSQL has been built and installed successfully.")
        click.echo("PostgreSQL has been built and installed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {e.cmd}")
        logger.error(f"Return code: {e.returncode}")
        logger.error(f"Command output:\n{e.output}")
        click.secho("Build failed. Check logs for details.", fg='red')
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unknown error during build: {e}")
        click.secho(f"Unknown error during build: {e}", fg='red')
        sys.exit(1)

@click.command(name='build')
@click.option('--clean', is_flag=True, help="Run 'make clean' before building")
def build_postgresql_cmd(clean: bool = False, implementation: str = "vanilla"):
    """
    CLI command: Builds and installs PostgreSQL from source.
    """
    build_postgresql(clean=clean, implementation=implementation)