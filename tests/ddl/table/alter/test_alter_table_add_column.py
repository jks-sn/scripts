# tests/ddl/table/create/test_alter_table_add_column.py

import pytest
import time

@pytest.mark.ddl
def test_alter_table_add_column(ddl_session, master_cluster, replica1_cluster):
    """
    Testing ALTER TABLE ADD COLUMN.
    """
    master_name = master_cluster.name
    replica1_name = replica1_cluster.name
    schema_name = master_cluster.replication_schema
    table_name = "test_add_column"

    # 1) Create table on master
    ddl_session.create_table(master_name, schema_name, table_name)
    time.sleep(1)

    # 2) Check table exists on replica1
    assert ddl_session.table_exists(replica1_name, schema_name, table_name), \
        "Table was not replicated before adding column."

    # 3) Add column
    ddl_session.add_column(master_name, schema_name, table_name, "new_column", "INTEGER", default_value=0)
    time.sleep(1)

    # 4) Check columns on replica1
    columns = ddl_session.get_table_columns(replica1_name, schema_name, table_name)
    col_names = [col["column_name"] for col in columns]
    assert "new_column" in col_names, "New column was not replicated to replica1"
