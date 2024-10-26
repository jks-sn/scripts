# tests/cascade/dml/test_multiple_data_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import cascade_dml_test

class TestMultipleDataReplication(BaseTest):
    @cascade_dml_test
    def test_multiple_data_replication(self):
        """Тест множественной вставки данных и их репликации до replica2."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = master['replication_table']

        # Вставка нескольких строк данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Data 1'), ('Data 2'), ('Data 3');
        """
        execute_sql(master['conn_params'], sql=insert_query, server_name=master['name'])

        # Ожидание репликации
        time.sleep(2)

        # Проверка данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE data IN ('Data 1', 'Data 2', 'Data 3');
        """
        results = execute_sql(replica2['conn_params'], sql=select_query, server_name=replica2['name'], fetch=True)

        # Проверки
        self.assertIsNotNone(results, "Нет данных на replica2")
        self.assertEqual(len(results), 3, "Количество строк не соответствует")
        replicated_data = [row[0] for row in results]
        for data in ['Data 1', 'Data 2', 'Data 3']:
            self.assertIn(data, replicated_data, f"'{data}' отсутствует на replica2")

        click.echo("Тест множественной вставки данных успешно пройден.")
