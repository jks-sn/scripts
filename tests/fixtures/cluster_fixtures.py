# tests/fixtures/cluster_fixtures.py

import pytest
from models.config import load_config

@pytest.fixture(scope="session")
def config():
    """
    Loads config.json once per session.
    """
    return load_config()

@pytest.fixture(scope="session")
def master_cluster(config):
    """
    Returns the 'master' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "master")

@pytest.fixture(scope="session")
def replica1_cluster(config):
    """
    Returns the 'replica1' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "replica1")

@pytest.fixture(scope="session")
def replica2_cluster(config):
    """
    Returns the 'replica2' cluster object from config.
    """
    return next(n for n in config.nodes if n.name == "replica2")
