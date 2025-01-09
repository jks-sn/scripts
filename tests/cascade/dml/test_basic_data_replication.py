# # tests/cascade/dml/test_basic_data_replication.py

# import unittest
# import time
# import click
# from tests.base_test import BaseTest
# from utils.execute import execute_sql
# from tests.test_tags import cascade_dml_test

# class TestBasicDataReplication(BaseTest):
#     @cascade_dml_test
#     def test_data_replication(self):
#         """Тест проверки репликации данных от мастера до replica2."""
#         master = self.clusters['master']
#         replica2 = self.clusters['replica2']

#         schema_name = master['replication_schema']
#         table_name = master['replication_table']

#         # Вставка данных на мастере
#         insert_query = f"""
#             INSERT INTO {schema_name}.{table_name} (data)
#             VALUES ('Test data replication');
#         """
#         execute_sql(conn_params=master['conn_params'], sql=insert_query, server_name=master['name'])

#         # Ожидание репликации
#         time.sleep(2)

#         # Проверка данных на replica2
#         select_query = f"""
#             SELECT data FROM {schema_name}.{table_name}
#             WHERE data = 'Test data replication';
#         """
#         results = execute_sql(conn_params=replica2['conn_params'], sql=select_query, server_name=replica2['name'], fetch=True)

#         # Проверки
#         self.assertIsNotNone(results, "Нет данных на replica2")
#         self.assertEqual(len(results), 1, "Количество строк не соответствует")
#         self.assertEqual(results[0][0], 'Test data replication', "Данные не совпадают")

#         click.echo("Тест репликации данных успешно пройден.")
