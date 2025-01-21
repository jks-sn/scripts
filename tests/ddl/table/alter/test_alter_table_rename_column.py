#tests/ddl/table/alter/test_alter_table_rename_column.py

import pytest
import time

@pytest.mark.ddl
def test_alter_table_rename_column(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_rename_column"
    old_col_name = "col_old"
    new_col_name = "col_new"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        columns_def={"id": "SERIAL PRIMARY KEY", old_col_name: "text"}
    )

    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name,
            columns_def={"id": "SERIAL PRIMARY KEY", old_col_name: "text"}
        )
    time.sleep(replication_wait_time)

    ddl_implementation.rename_column(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        old_column_name=old_col_name,
        new_column_name=new_col_name
    )

    time.sleep(replication_wait_time)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, table_name)
    col_names = [c["column_name"] for c in columns]

    assert new_col_name in col_names, f"Column '{new_col_name}' did not appear on the replica."
    assert old_col_name not in col_names, f"Old column '{old_col_name}' should not exist on the replica."
