# # tests/ddl/table/create/test_drop_table.py

# import pytest
# import time

# @pytest.mark.ddl
# def test_drop_table(local_setup, ddl_implementation, master_node, replica1_node):

#     master_name = master_node.name
#     replica_name = replica1_node.name
#     schema_name = master_node.replication_schema
#     table_name = "test_drop_table"

#     ddl_implementation.create_table(
#         node_name=master_name,
#         schema_name=schema_name,
#         table_name=table_name)

#     time.sleep(1)

#     assert ddl_implementation.table_exists(replica_name, schema_name, table_name), \
#         "Table was not replicated to the replica."

#     ddl_implementation.drop_table(master_name, schema_name, table_name)
#     time.sleep(1)

#     assert not ddl_implementation.table_exists(replica_name, schema_name, table_name), \
#         "Table was not dropped on the replica."
