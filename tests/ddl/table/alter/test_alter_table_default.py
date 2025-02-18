# tests/ddl/table/alter/test_alter_table_default.py

import pytest
import time

@pytest.mark.ddl
def test_alter_table_alter_column_default(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a table (`id serial primary key, data text`).
    2) Execute `ALTER TABLE ... ALTER COLUMN data SET DEFAULT 'Hello default!';`
    3) Insert a new row (without specifying `data`).
    4) Verify that on the master, `data='Hello default!'`, and the same on the replica.
    5) Execute `ALTER TABLE ... ALTER COLUMN data SET DEFAULT 'Another default';`
    6) Insert another row and verify the result.
    7) Finally, `DROP DEFAULT` (optional step).
    """
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_default"

    ddl_implementation.create_table(
        master_name,
        schema_name,
        table_name,
        columns_def={"id": "SERIAL PRIMARY KEY", "data": "TEXT"}
    )
    time.sleep(replication_wait_time)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name
        )

    alter_sql_1 = f"""
    ALTER TABLE {schema_name}.{table_name}
    ALTER COLUMN data
    SET DEFAULT 'Hello default!';
    """
    ddl_implementation._execute(master_name, alter_sql_1)
    time.sleep(replication_wait_time)

    row1 = {"id": 1}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row1)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)

    assert len(rows_replica) == 1
    assert rows_replica[0]["id"] == 1
    assert rows_replica[0]["data"] == "Hello default!"

    alter_sql_2 = f"""
    ALTER TABLE {schema_name}.{table_name}
    ALTER COLUMN data
    SET DEFAULT 'Another default';
    """
    ddl_implementation._execute(master_name, alter_sql_2)
    time.sleep(replication_wait_time)

    row2 = {"id": 2}
    ddl_implementation.insert_into_table(master_name, schema_name, table_name, row2)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, table_name)
    assert len(rows_replica) == 2
    row2_data = next(r for r in rows_replica if r["id"] == 2)
    assert row2_data["data"] == "Another default"
