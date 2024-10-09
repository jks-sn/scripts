# tests/cascade/dml/test_delete_data_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import dml_test

class TestDeleteDataReplication(BaseTest):
    @dml_test
    def test_delete_data_replication(self):
        """Тест удаления данных и их репликации до replica2."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = master['replication_table']

        # Вставка данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Data to Delete') RETURNING id;
        """
        result = execute_sql(master['conn_params'], insert_query, server_name=master['name'], fetch=True)
        inserted_id = result[0][0]

        # Удаление данных на мастере
        delete_query = f"""
            DELETE FROM {schema_name}.{table_name}
            WHERE id = {inserted_id};
        """
        execute_sql(master['conn_params'], delete_query, server_name=master['name'])

        # Ожидание репликации
        time.sleep(2)

        # Проверка отсутствия данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE id = {inserted_id};
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)

        # Проверки
        self.assertEqual(len(results), 0, "Данные не были удалены на replica2")

        click.echo("Тест удаления данных успешно пройден.")
