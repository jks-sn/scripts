# tests/fixtures/cluster_fixtures.py

import pytest

@pytest.fixture(scope="session")
def master_node(config):
    """
    Returns the 'master' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "master")

@pytest.fixture(scope="session")
def replica1_node(config):
    """
    Returns the 'replica1' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "replica1")

@pytest.fixture(scope="session")
def replica2_node(config):
    """
    Returns the 'replica2' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "replica2")
