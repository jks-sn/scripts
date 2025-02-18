# tests/ddl/table/alter/test_alter_table_add_constraint.py

import pytest
import time
import psycopg2

@pytest.mark.ddl
def test_alter_table_add_constraint_check(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table (`id INT, val INT`).
    2) Execute `ALTER TABLE ADD CONSTRAINT check_val_positive CHECK (val > 0)`.
    3) Verify that the constraint appears on the replica.
    4) Attempt to insert invalid data on the master (`val <= 0`) — it should fail.
    5) Insert valid data and verify the replica.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_add_check"

    ddl_implementation.create_table(
        master_name,
        schema_name,
        table_name,
        columns_def={"id": "SERIAL", "val": "INT"}
    )
    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name
        )

    add_constraint_sql = f"""
    ALTER TABLE {schema_name}.{table_name}
    ADD CONSTRAINT check_val_positive
    CHECK (val > 0);
    """
    ddl_implementation._execute(master_name, add_constraint_sql)
    time.sleep(replication_wait_time)

    check_constraints_sql = f"""
    SELECT conname, contype
    FROM pg_constraint
    WHERE conrelid = '{schema_name}.{table_name}'::regclass
      AND conname = 'check_val_positive';
    """
    rows = ddl_implementation._execute(replica_name, check_constraints_sql, fetch=True)
    assert rows, "Constraint 'check_val_positive' not found on replica."
    try:
        ddl_implementation.insert_into_table(
            master_name, schema_name, table_name,
            data={"val": -10}
        )
        pytest.fail("Expected an error due to check constraint (val > 0), but no error was raised.")
    except psycopg2.Error:
        pass

    good_row = {"val": 100}
    ddl_implementation.insert_into_table(
        master_name, schema_name, table_name, good_row
    )
    time.sleep(replication_wait_time)

    rows_on_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows_on_replica) == 1
    assert rows_on_replica[0]["val"] == 100
