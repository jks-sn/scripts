#tests/ddl/table/alter/test_alter_table_alter_column_type.py

import pytest
import time


@pytest.mark.ddl
def test_alter_table_alter_column_type(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table (`id SERIAL PRIMARY KEY, col_to_alter TEXT`).
    2) Execute `ALTER TABLE ALTER COLUMN col_to_alter TYPE VARCHAR(255)` on the master.
    3) Verify that the column type change is replicated.
    4) Check that the column exists on the replica with the expected type (`VARCHAR(255)`).
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_alter_column_type"
    col_name = "col_to_alter"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        columns_def={"id": "SERIAL PRIMARY KEY", col_name: "text"}
    )
    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name,
            columns_def={"id": "SERIAL PRIMARY KEY", col_name: "text"}
        )
    time.sleep(replication_wait_time)

    ddl_implementation.alter_column_type(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=col_name,
        new_type="VARCHAR(255)"
    )

    time.sleep(replication_wait_time)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, table_name)
    altered_col = next((c for c in columns if c["column_name"] == col_name), None)
    assert altered_col is not None, f"Column {col_name} was not found on the replica!"
    actual_type = altered_col["data_type"]
    assert actual_type in ("character varying"), f"Type on the replica '{actual_type}' does not match expected 'integer'."