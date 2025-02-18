# tests/ddl/table/create/test_create_table_inheritance.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_with_inheritance_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create `parent_table` on the master,
    2) Create `child_table` inheriting from `parent_table`,
    3) Verify that both tables are created on the replica,
    4) Insert data into `child_table` (including fields from the parent),
    5) Verify that the data has been replicated.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema

    parent_table = "inherit_parent"
    child_table = "inherit_child"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=parent_table,
        columns_def={"id": "SERIAL PRIMARY KEY", "parent_col": "TEXT"}
    )
    time.sleep(replication_wait_time)
    assert ddl_implementation.table_exists(replica_name, schema_name, parent_table)

    create_child_sql = f"""
        CREATE TABLE {schema_name}.{child_table} (
            child_col INT
        )
        INHERITS ({schema_name}.{parent_table});
    """
    ddl_implementation._execute(master_name, create_child_sql)
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, child_table)

    row_data = {"id": 1, "parent_col": "some parent data", "child_col": 42}
    ddl_implementation.insert_into_table(master_name, schema_name, child_table, row_data)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, child_table)
    assert len(rows_replica) == 1, "Row not found in inherited child_table on replica"
    inserted = rows_replica[0]
    assert inserted["child_col"] == 42
    assert inserted["parent_col"] == "some parent data"
