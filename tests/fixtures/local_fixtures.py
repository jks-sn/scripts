# tests/fixtures/local_fixtures.py

import pytest
from commands.clean_replication import clean_replication
from commands.replication import setup_replication
from utils.log_handler import logger

@pytest.fixture(scope="function")
def local_setup(request, ddl_implementation):
    logger.debug("[local_setup] PREPARE replication before test.")
    clean_replication()

    ddl = False
    cascade = False

    if request.node.get_closest_marker("ddl"):
        ddl = True
    if request.node.get_closest_marker("cascade"):
        cascade = True

    logger.debug("[local_setup] SETUP replication with implementation=")
    setup_replication(ddl=ddl, cascade=cascade)

    yield

    logger.debug("[local_setup] CLEAN replication after test.")
    clean_replication()
