# tests/ddl/table/restart/test_create_table_then_restart.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_then_restart_cluster(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table on the master;
    2) Check that it appears on the replica;
    3) Stop and restart all clusters;
    4) Insert data on the master;
    5) Ensure that the data is replicated to the replica.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_create_table_restart"

    ddl_implementation.create_table(master_name, schema_name, table_name)
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, table_name), \
        f"Table '{schema_name}.{table_name}' was not replicated to '{replica_name}' before restart."

    ddl_implementation.stop_cluster()
    ddl_implementation.start_cluster()
    time.sleep(replication_wait_time)

    row_data = {"id": 100, "data": "Hello after restart"}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row_data)

    time.sleep(replication_wait_time)

    rows_on_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    inserted = [r for r in rows_on_replica if r["id"] == 100]
    assert inserted, f"Row with id=100 not replicated after cluster restart in table '{table_name}'."
    assert inserted[0]["data"] == "Hello after restart"
