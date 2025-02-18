# tests/ddl/table/create/test_create_table_generated_column.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_with_generated_column_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table with a generated column (`gen_val` computed as `base_val * 2`).
    2) Verify that the table is replicated to the replica.
    3) Insert a row into the master (`base_val = 10`).
    4) Verify that the row is replicated to the replica.
    5) Ensure that the generated column (`gen_val`) is correctly computed and replicated (`gen_val = 20`).
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_table_generated_col"

    create_table_sql = f"""
    CREATE TABLE {schema_name}.{table_name} (
        base_val INT,
        gen_val  INT GENERATED ALWAYS AS (base_val * 2) STORED
    );
    """
    ddl_implementation._execute(master_name, create_table_sql)
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, table_name)

    row_data = {"base_val": 10}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row_data)
    time.sleep(replication_wait_time)

    rows = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows) == 1
    assert rows[0]["base_val"] == 10
    assert rows[0]["gen_val"] == 20, "Generated column not replicated or computed incorrectly"
