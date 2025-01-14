#commands/build.py

import subprocess
import sys
import os
import click
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from utils.log_handler import logger

def build_postgresql(clean: bool = False,  implementation: str = "vanilla"):
	"""
	Builds and installs PostgreSQL from source.

	:param clean: If True, runs 'make clean' before building.
	"""
	config = load_config()
	ddl_impl = get_ddl_implementation(db_type="postgresql", implementation_type=implementation, config=config)
	ddl_impl.build_source()

	click.echo("PostgreSQL has been built and installed successfully.")

@click.command(name='build')
@click.option('--clean', is_flag=True, help="Run 'make clean' before building")
def build_postgresql_cmd(clean: bool = False, implementation: str = "vanilla"):
	"""
	CLI command: Builds and installs PostgreSQL from source.
	"""
	build_postgresql(clean=clean, implementation=implementation)