# tests/fixtures/global_fixtures.py

import pytest
import logging

from utils.log_handler import logger

@pytest.fixture(scope="session", autouse=True)
def global_setup(request, ddl_implementation):
    """
    Session-scoped fixture that runs once.
    Then yields, and at the end does optional teardown (stop clusters).
    """
    logger.debug("[global_setup]")

    yield

    logger.debug("[global_setup] Teardown")

