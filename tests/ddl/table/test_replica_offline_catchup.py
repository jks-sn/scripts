# tests/ddl/table/test_replica_offline_catchup.py

import pytest
import time

@pytest.mark.ddl
def test_replica_offline_catchup(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table on the master (replica1 is still running, everything is fine);
    2) Stop replica1;
    3) Perform an ALTER TABLE operation on the master (add a column or rename the table) and insert data;
    4) Start replica1;
    5) Verify that the replica has caught up with all changes (both structure and data).
    """
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_offline_replica"

    ddl_implementation.create_table(master_name, schema_name, table_name)
    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name
        )


    ddl_implementation.stop_server(replica_name)

    ddl_implementation.add_column(master_name, schema_name, table_name, "offline_col", "text")
    row_data = {"id": 1, "data": "before start replica", "offline_col": "added while replica down"}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row_data)

    ddl_implementation.start_server(replica_name)
    time.sleep(replication_wait_time * 2)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, table_name)
    col_names = [c["column_name"] for c in columns]
    assert "offline_col" in col_names, "Column 'offline_col' did not replicate after replica re-start."

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    row_found = next((r for r in rows_replica if r["id"] == 1), None)
    assert row_found, "Row with id=1 not found on replica after catch-up."
    assert row_found["offline_col"] == "added while replica down", "offline_col mismatch."
