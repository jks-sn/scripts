# tests/cascade/dml/test_update_data_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import dml_test

class TestUpdateDataReplication(BaseTest):
    @dml_test
    def test_update_data_replication(self):
        """Тест обновления данных и их репликации до replica2."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = master['replication_table']

        # Вставка данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Old Data') RETURNING id;
        """
        result = execute_sql(master['conn_params'], sql=insert_query, server_name=master['name'], fetch=True)
        inserted_id = result[0][0]

        # Обновление данных на мастере
        update_query = f"""
            UPDATE {schema_name}.{table_name}
            SET data = 'Updated Data'
            WHERE id = {inserted_id};
        """
        execute_sql(master['conn_params'], sql=update_query, server_name=master['name'])

        # Ожидание репликации
        time.sleep(2)

        # Проверка обновленных данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE id = {inserted_id};
        """
        results = execute_sql(replica2['conn_params'], sql=select_query, server_name=replica2['name'], fetch=True)

        # Проверки
        self.assertIsNotNone(results, "Нет данных на replica2")
        self.assertEqual(len(results), 1, "Количество строк не соответствует")
        self.assertEqual(results[0][0], 'Updated Data', "Данные не совпадают")

        click.echo("Тест обновления данных успешно пройден.")
