# tests/ddl/table/test_multi_ddl_sequence.py

import pytest
import time

@pytest.mark.ddl
def test_multiple_ddl_operations_in_sequence(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):

    """ We check that consecutive DDL operations on the same table are correctly replicated:

        CREATE TABLE
        ALTER TABLE ADD COLUMN
        ALTER TABLE RENAME COLUMN
        ALTER TABLE DROP COLUMN
        RENAME TABLE
        DROP TABLE
        At each stage, we ensure that the replica does not lag behind. """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema

    old_table_name = "test_multi_ddl"
    new_table_name = "test_multi_ddl_renamed"
    col1 = "col_add_me"
    col2_old = "col_rename_me"
    col2_new = "col_renamed"

    # 1) CREATE TABLE
    ddl_implementation.create_table(
        master_name, schema_name, old_table_name,
        columns_def={"id": "SERIAL PRIMARY KEY", col2_old: "INT"}
    )
    time.sleep(replication_wait_time)
    assert ddl_implementation.table_exists(replica_name, schema_name, old_table_name), "Table not replicated."


    ddl_implementation.add_column(
        node_name=master_name,
        schema_name=schema_name,
        table_name=old_table_name,
        column_name=col1,
        column_type="TEXT",
        default_value=None
    )
    time.sleep(replication_wait_time)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, old_table_name)
    col_names = [c["column_name"] for c in columns]
    assert col1 in col_names, "New column not replicated to replica."


    ddl_implementation.rename_column(master_name, schema_name, old_table_name, col2_old, col2_new)
    time.sleep(replication_wait_time)
    columns = ddl_implementation.get_table_columns(replica_name, schema_name, old_table_name)
    col_names = [c["column_name"] for c in columns]
    assert col2_new in col_names and col2_old not in col_names, "Column rename not replicated properly."


    ddl_implementation.drop_column(master_name, schema_name, old_table_name, col1)
    time.sleep(replication_wait_time)
    columns = ddl_implementation.get_table_columns(replica_name, schema_name, old_table_name)
    col_names = [c["column_name"] for c in columns]
    assert col1 not in col_names, "Dropped column is still on replica."

    ddl_implementation.rename_table(master_name, schema_name, old_table_name, new_table_name)
    time.sleep(replication_wait_time)
    assert not ddl_implementation.table_exists(replica_name, schema_name, old_table_name), "Old table name still on replica."
    assert ddl_implementation.table_exists(replica_name, schema_name, new_table_name), "Renamed table not found on replica."

    ddl_implementation.drop_table(master_name, schema_name, new_table_name)
    time.sleep(replication_wait_time)
    assert not ddl_implementation.table_exists(replica_name, schema_name, new_table_name), "Table not dropped on replica."
