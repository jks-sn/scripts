# tests/ddl/table/create/test_create_table_partitioned.py

import pytest
import time

@pytest.mark.ddl
def test_create_table_partitioned_and_data(local_setup, ddl_implementation, master_node, replica1_node, replication_wait_time):
    """
    1) Create a partitioned table (`partitioned_parent`) with range-based partitioning on `id`.
    2) Create two partitions:
       - `partition_p1` for values from 1 to 99.
       - `partition_p2` for values from 100 to 999999.
    3) Verify that the parent table and both partitions are replicated to the replica.
    4) Insert two rows into the parent table (`id = 50` and `id = 150`).
    5) Verify that the inserted rows are correctly replicated and accessible via the parent table on the replica.
    """

    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema

    parent_table = "partitioned_parent"
    part1_table = "partition_p1"
    part2_table = "partition_p2"

    create_parent_sql = f"""
        CREATE TABLE {schema_name}.{parent_table} (
            id INT,
            val TEXT
        )
        PARTITION BY RANGE (id);
    """
    ddl_implementation._execute(master_name, create_parent_sql)

    create_p1_sql = f"""
        CREATE TABLE {schema_name}.{part1_table}
            PARTITION OF {schema_name}.{parent_table}
            FOR VALUES FROM (1) TO (100);
    """
    create_p2_sql = f"""
        CREATE TABLE {schema_name}.{part2_table}
            PARTITION OF {schema_name}.{parent_table}
            FOR VALUES FROM (100) TO (1000000);
    """

    ddl_implementation._execute(master_name, create_p1_sql)
    ddl_implementation._execute(master_name, create_p2_sql)

    time.sleep(replication_wait_time)

    for tname in [parent_table, part1_table, part2_table]:
        assert ddl_implementation.table_exists(replica_name, schema_name, tname), \
            f"Partition table '{tname}' not found on replica."

    row_data_1 = {"id": 50, "val": "in p1"}
    row_data_2 = {"id": 150, "val": "in p2"}

    ddl_implementation.insert_into_table(master_name, schema_name, parent_table, row_data_1)
    ddl_implementation.insert_into_table(master_name, schema_name, parent_table, row_data_2)

    time.sleep(replication_wait_time)

    rows_replica = ddl_implementation.select_all(replica_name, schema_name, parent_table)
    assert len(rows_replica) == 2, f"Expected 2 rows in parent_table, got {len(rows_replica)}"
    sorted_rows = sorted(rows_replica, key=lambda r: r["id"])
    assert sorted_rows[0]["id"] == 50
    assert sorted_rows[1]["id"] == 150
