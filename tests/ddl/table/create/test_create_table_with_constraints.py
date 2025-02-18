# tests/ddl/table/create/test_create_table_with_constraints.py

import pytest
import time
import psycopg2

@pytest.mark.ddl
def test_create_table_with_constraints_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table with constraints:
       - `id SERIAL PRIMARY KEY`
       - `val INT` with a `CHECK (val > 0)` constraint.
    2) Verify that the table is replicated to the replica.
    3) Insert a valid row (`val = 10`) into the master table.
    4) Verify that the row is correctly replicated to the replica.
    5) (Optional) Test inserting invalid data (`val <= 0`) and ensure it fails on the master.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_create_table_constraints"

    create_table_sql = f"""
    CREATE TABLE {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        val INT,
        CONSTRAINT val_positive CHECK (val > 0)
    );
    """
    ddl_implementation._execute(master_name, create_table_sql)
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, table_name)

    valid_data = {"val": 10}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, valid_data)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows_replica) == 1
    assert rows_replica[0]["val"] == 10

# Maybe should test invalid data
