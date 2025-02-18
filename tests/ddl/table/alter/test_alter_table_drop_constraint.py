# tests/ddl/table/alter/test_alter_table_drop_constraint.py

import pytest
import time
import psycopg2

@pytest.mark.ddl
def test_alter_table_drop_constraint(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table and add a CHECK constraint (`val > 0`).
    2) Verify that the constraint exists on the replica.
    3) Execute `ALTER TABLE ... DROP CONSTRAINT ...`
    4) Verify that the constraint is removed on the replica.
    5) Insert invalid data that previously would have been rejected—now it should be accepted.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_drop_constraint"

    create_sql = f"""
    CREATE TABLE {schema_name}.{table_name} (
        id SERIAL,
        val INT,
        CONSTRAINT check_val_positive CHECK (val > 0)
    );
    """
    ddl_implementation._execute(master_name, create_sql)
    time.sleep(replication_wait_time)

    con_check_sql = f"""
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = '{schema_name}.{table_name}'::regclass
      AND conname = 'check_val_positive';
    """
    constraints_before = ddl_implementation._execute(replica_name, con_check_sql, fetch=True)
    assert constraints_before, "Constraint not replicated to replica."

    drop_sql = f"""
    ALTER TABLE {schema_name}.{table_name}
    DROP CONSTRAINT check_val_positive;
    """
    ddl_implementation._execute(master_name, drop_sql)
    time.sleep(replication_wait_time)

    constraints_after = ddl_implementation._execute(replica_name, con_check_sql, fetch=True)
    assert not constraints_after, "Constraint still exists on replica after DROP."

    row_neg = {"val": -100}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row_neg)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    inserted_row = next((r for r in rows_replica if r["val"] == -100), None)
    assert inserted_row, "Row with val=-100 not found on replica after dropping constraint."
