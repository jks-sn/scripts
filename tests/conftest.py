# tests/conftest.py

import json
import pytest
import time

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

# --------------------
# Метрики тестов
# --------------------

def pytest_sessionstart(session):
    session.config._my_metrics = {
        "start_time": time.time(),
        "passed": 0,
        "failed": 0,
        "skipped": 0
    }

def pytest_runtest_makereport(item, call):

    if call.when == "call":

        metrics = item.config._my_metrics

        if call.excinfo is None:
            metrics["passed"] += 1
        else:
            exc_type = call.excinfo.typename
            if exc_type == "Skipped":
                metrics["skipped"] += 1
            else:
                metrics["failed"] += 1

def pytest_sessionfinish(session, exitstatus):
    metrics = session.config._my_metrics
    total_time = time.time() - metrics["start_time"]

    p = metrics["passed"]
    f = metrics["failed"]
    s = metrics["skipped"]
    total = p + f + s

    print("\n=== [Test session metrics] ===")
    print(f"Total tests run: {total}")
    print(f"  Passed:  {p}")
    print(f"  Failed:  {f}")
    print(f"  Skipped: {s}")
    print(f"Total run time: {total_time:.2f} seconds")
    print("================================\n")

    results_dict = {
        "total": total,
        "passed": p,
        "failed": f,
        "skipped": s,
        "time_seconds": total_time,
    }
    with open("pytest_metrics.json", "w") as fjson:
        json.dump(results_dict, fjson, indent=2)
