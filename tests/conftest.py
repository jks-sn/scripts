# tests/conftest.py

import pytest
from factories.ddl_factory import get_ddl_implementation
from models.config import load_config
from tests.fixtures.cluster_fixtures import *
from tests.fixtures.global_fixtures import *
from tests.fixtures.local_fixtures import *

def pytest_configure(config):
    config.addinivalue_line("markers", "ddl: Marker for tests, that need ddl replication")
    config.addinivalue_line("markers", "dml: Marker for tests, that need dml replication")
    config.addinivalue_line("markers", "cascade: Marker for tests, that need cascade replication")

def pytest_addoption(parser):
    parser.addini("replication_wait_time", "Time to wait (in seconds) for replication", default="1")

@pytest.fixture(scope="session")
def config():
    """
    Loads the configuration (e.g. from config.json) once per session.
    Returns the config object that contains cluster/node info.
    """
    return load_config()

@pytest.fixture(scope="session")
def ddl_implementation(request, config):
    """
    Returns the DDL implementation chosen via --implementation CLI option.
    Defaults to 'vanilla' if not specified.
    """
    return get_ddl_implementation("postgresql", config)

@pytest.fixture(scope="session")
def replication_wait_time(pytestconfig):
    """Return the replication wait time (int)."""
    return int(pytestconfig.getini("replication_wait_time"))