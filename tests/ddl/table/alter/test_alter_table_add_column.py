# tests/ddl/table/create/test_alter_table_add_column.py

import pytest
import time

from utils.execute import execute_sql

@pytest.mark.ddl
def test_alter_table_add_column(local_setup, ddl_implementation, master_node, replica1_node):
    """
    Testing ALTER TABLE ADD COLUMN.
    Assuming that CREATE TABLE might not replicate.
    Therefore, we manually create the table on both the master and the replica (if necessary).
    """
    master_name = master_node.name
    replica_name = replica1_node.name
    schema_name = master_node.replication_schema
    table_name = "test_alter_table_add_column"

    ddl_implementation.create_table(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name
    )

    time.sleep(1)

    if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
        ddl_implementation.create_table(
            node_name=replica_name,
            schema_name=schema_name,
            table_name=table_name
        )

    new_col_name = "new_column"
    ddl_implementation.add_column(
        node_name=master_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=new_col_name,
        column_type="INTEGER",
        default_value=0
    )

    time.sleep(1)

    columns = ddl_implementation.get_table_columns(replica_name, schema_name, table_name)
    col_names = [col["column_name"] for col in columns]
    assert new_col_name in col_names, \
        f"The new column '{new_col_name}' did not appear on the replica"


    insert_sql = f"""
    INSERT INTO {schema_name}.{table_name} (id, data, new_column)
    VALUES (2, 'Hello from master2', 999);
    """
    execute_sql(
        conn_params=ddl_implementation.node_conn[master_name],
        sql=insert_sql,
        server_name=master_name,
        autocommit=True
    )

    time.sleep(5)

    rows = ddl_implementation.select_all(replica_name, schema_name, table_name)

    inserted_row = next((r for r in rows if r["id"] == 2), None)
    assert inserted_row is not None, "The row with id=2 was not replicated."
    assert inserted_row[new_col_name] == 999, \
        f"The column {new_col_name} has an incorrect value: {inserted_row[new_col_name]}"
