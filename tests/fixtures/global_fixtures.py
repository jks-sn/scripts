# tests/fixtures/global_fixtures.py

import pytest
import logging

from commands.build import build_postgresql
from commands.cluster import init_cluster, start_cluster, stop_cluster
from factories.ddl_factory import get_ddl_implementation
from utils.config_loader import load_config
from utils.log_handler import logger

@pytest.fixture(scope="session", autouse=True)
def global_setup(request, implementation):
    """
    Session-scoped fixture that runs once.
    Then yields, and at the end does optional teardown (stop clusters).
    """
    logger.debug("[global_setup] Building, init, start clusters.")

    start_cluster()

    yield

    logger.debug("[global_setup] Teardown => stop clusters.")
    stop_cluster()


@pytest.fixture(scope="session")
def ddl_session(implementation):
    """
    Creates one DDLInterface instance for the entire test session.
    """
    config = load_config()
    ddl_impl = get_ddl_implementation("postgresql", implementation, config)
    return ddl_impl
