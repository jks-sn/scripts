# tests/conftest.py

import pytest
from utils.config_loader import load_config
from factories.ddl_factory import get_ddl_implementation

def pytest_configure(config):
    config.addinivalue_line("markers", "ddl: Marker for tests, that need ddl replication")
    config.addinivalue_line("markers", "cascade: Marker for tests, that need cascade replication")

def pytest_addoption(parser):
    """
    Adds a custom CLI option to specify the DDL implementation type.
    Example usage: pytest --implementation=ddl_patch
    """
    parser.addoption(
        "--implementation",
        action="store",
        default="vanilla",
        help="Type of DDL implementation to test (e.g. ddl_patch, vanilla, and others)."
    )

@pytest.fixture(scope="session")
def implementation(request):
    """
    Returns the DDL implementation chosen via --implementation CLI option.
    Defaults to 'vanilla' if not specified.
    """
    return request.config.getoption("--implementation")

from tests.fixtures.cluster_fixtures import *
from tests.fixtures.global_fixtures import *
from tests.fixtures.local_fixtures import *