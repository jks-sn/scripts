# tests/ddl/table/create/test_create_table_like.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_with_like_clause_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create an original table (`test_like_original`) with columns (`id INT, txt TEXT`).
    2) Verify that the table is replicated to the replica.
    3) Create a new table (`test_like_clone`) using `CREATE TABLE ... (LIKE original_table INCLUDING ALL)`.
    4) Verify that the cloned table appears on the replica.
    5) Insert a row into the cloned table on the master.
    6) Verify that the inserted row is replicated correctly to the replica.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema

    original_table = "test_like_original"
    like_table = "test_like_clone"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=original_table,
        columns_def={"id": "INT", "txt": "TEXT"}
    )
    time.sleep(replication_wait_time)
    assert ddl_implementation.table_exists(replica_name, schema_name, original_table)

    like_sql = f"CREATE TABLE {schema_name}.{like_table} (LIKE {schema_name}.{original_table} INCLUDING ALL);"
    ddl_implementation._execute(master_name, like_sql)
    time.sleep(replication_wait_time)

    assert ddl_implementation.table_exists(replica_name, schema_name, like_table)

    # Вставка
    row_data = {"id": 777, "txt": "like cloned row"}
    ddl_implementation.insert_into_table(master_name, schema_name, like_table, row_data)
    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, like_table)
    assert len(rows_replica) == 1
    assert rows_replica[0]["id"] == 777
    assert rows_replica[0]["txt"] == "like cloned row"
