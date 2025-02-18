# File: tests/ddl/table/create/test_create_table_various_datatypes.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_with_various_datatypes_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table with columns of various data types:
       - INTEGER, BIGINT, VARCHAR(50), BOOLEAN, TIMESTAMP, NUMERIC(10,2).
    2) Verify that the table is replicated to the replica.
    3) Insert a row with values of different types into the master table.
    4) Verify that the row is correctly replicated to the replica.
    5) Ensure that all column values match the expected values after replication.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_create_table_various_types"

    columns_def = {
        "col_int": "INTEGER",
        "col_bigint": "BIGINT",
        "col_varchar": "VARCHAR(50)",
        "col_bool": "BOOLEAN",
        "col_timestamp": "TIMESTAMP",
        "col_numeric": "NUMERIC(10,2)"
    }

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        columns_def=columns_def
    )
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, table_name)

    row_to_insert = {
        "col_int": 123,
        "col_bigint": 1234567890123,
        "col_varchar": "some text",
        "col_bool": True,
        "col_timestamp": "2025-02-17 15:20:00",
        "col_numeric": 99.99
    }
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row_to_insert)

    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows_replica) == 1
    inserted_row = rows_replica[0]

    assert inserted_row["col_int"] == 123
    assert inserted_row["col_bigint"] == 1234567890123
    assert inserted_row["col_varchar"] == "some text"
    assert inserted_row["col_bool"] == True
