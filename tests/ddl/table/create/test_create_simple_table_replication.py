# tests/ddl/table/create/test_create_simple_table.py

import pytest
import time

from utils.execute import execute_sql

@pytest.mark.ddl
def test_create_simple_table(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    The test checks the replication of CREATE TABLE (if supported)
    and verifies that the table is actually "live": performing INSERT on the master,
    reading data on the replica.
    """
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_create_simple_table"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name
    )

    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        pytest.fail("CREATE TABLE was not replicated. The extension/logic may not support this operation.")

    ddl_implementation.insert_into_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        data={"id": 1, "data": "Hello from master"}
        )

    time.sleep(replication_wait_time)

    rows = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    assert rows[0]["id"] == 1
    assert rows[0]["data"] == "Hello from master"
