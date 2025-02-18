#tests/ddl/table/alter/test_alter_table_drop_column.py

import pytest
import time

@pytest.mark.ddl
def test_alter_table_drop_column(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table (`id SERIAL PRIMARY KEY, drop_me TEXT`).
    2) Execute `ALTER TABLE DROP COLUMN drop_me` on the master.
    3) Verify that the column is removed on the replica.
    4) Check that the column `drop_me` no longer exists in the replica's schema.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_drop_column"
    col_name = "drop_me"

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

    ddl_implementation.drop_column(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=col_name
    )

    time.sleep(replication_wait_time)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, table_name)
    col_names = [c["column_name"] for c in columns]
    assert col_name not in col_names, f"Column '{col_name}' still exists on the replica!"

