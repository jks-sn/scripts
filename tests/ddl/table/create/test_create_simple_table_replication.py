# tests/ddl/table/create/test_create_simple_table.py

import pytest
import time

@pytest.mark.ddl
def test_create_simple_table(local_setup, ddl_implementation, master_cluster, replica1_cluster):
    """
    Testing creation of a table on 'master' cluster, verifying replication on 'replica1'.
    No direct SQL calls, using ddl_implementation instead.
    """

    master_name = master_cluster.name
    replica1_name = replica1_cluster.name

    schema_name = master_cluster.replication_schema
    table_name = "test_create_simple_table"

    ddl_implementation.create_table(master_name, schema_name, table_name)
    time.sleep(1)

    assert ddl_implementation.table_exists(replica1_name, schema_name, table_name), \
        f"Table {schema_name}.{table_name} was not replicated on {replica1_name}."
