#tests/ddl/table/alter/test_alter_table_rename_table.py

import pytest
import time

@pytest.mark.ddl
def test_alter_table_rename_table(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    old_table_name = "test_alter_table_rename_table"
    new_table_name = "test_alter_table_rename_table_new"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=old_table_name
    )

    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, old_table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=old_table_name
        )

    ddl_implementation.rename_table(
        node_name=master_name,
        schema_name=schema_name,
        old_table_name=old_table_name,
        new_table_name=new_table_name
    )

    time.sleep(replication_wait_time)

    exists_old = ddl_implementation.table_exists(replica_name, schema_name, old_table_name)
    exists_new = ddl_implementation.table_exists(replica_name, schema_name, new_table_name)

    assert not exists_old, f"Table '{old_table_name}' should no longer exist on the replica."
    assert exists_new, f"Table '{new_table_name}' should appear on the replica."

