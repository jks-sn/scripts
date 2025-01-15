# tests/ddl/table/create/test_drop_table.py

import pytest
import time

@pytest.mark.ddl
def test_drop_table(local_setup, ddl_implementation, master_cluster, replica1_cluster):
    schema_name = master_cluster.replication_schema
    master_name = master_cluster.name
    replica_name = replica1_cluster.name

    table_name = "test_drop_table"

    # 1) Create table on master
    ddl_implementation.create_table(master_name, schema_name, table_name)
    time.sleep(1)

    # 2) Check it on replica
    assert ddl_implementation.table_exists(replica_name, schema_name, table_name), \
        "Table was not replicated to the replica."

    # 3) Drop the table on master
    ddl_implementation.drop_table(master_name, schema_name, table_name)
    time.sleep(1)

    # 4) Ensure it's gone on replica
    assert not ddl_implementation.table_exists(replica_name, schema_name, table_name), \
        "Table was not dropped on the replica."
