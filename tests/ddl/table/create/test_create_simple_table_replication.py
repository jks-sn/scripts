# # tests/ddl/table/create/test_create_simple_table.py

# import pytest
# import time

# from utils.execute import execute_sql

# @pytest.mark.ddl
# def test_create_simple_table(local_setup, ddl_implementation, master_node, replica1_node):
#     """
#     The test checks the replication of CREATE TABLE (if supported)
#     and verifies that the table is actually "live": performing INSERT on the master,
#     reading data on the replica.
#     """
#     master_name = master_node.name
#     replica_name = replica1_node.name
#     schema_name = master_node.replication_schema
#     table_name = "test_create_simple_table"

#     ddl_implementation.create_table(
#         node_name=master_name,
#         schema_name=schema_name,
#         table_name=table_name
#     )

#     time.sleep(1)

#     if not ddl_implementation.table_exists(replica_name, schema_name, table_name):
#         pytest.fail("CREATE TABLE was not replicated. The extension/logic may not support this operation.")

#     insert_sql = f"""
#     INSERT INTO {schema_name}.{table_name} (id, data)
#     VALUES (1, 'Hello from master');
#     """
#     execute_sql(
#         conn_params=master_node.conn_params,
#         sql=insert_sql,
#         server_name=master_name
#     )

#     time.sleep(1)

#     rows = ddl_implementation.select_all(replica_name, schema_name, table_name)
#     assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
#     assert rows[0]["id"] == 1
#     assert rows[0]["data"] == "Hello from master"
