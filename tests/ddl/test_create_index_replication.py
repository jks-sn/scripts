# tests/logical_ddl/ddl/test_create_index_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import logical_ddl_test

class TestCreateIndexReplication(BaseTest):
    @logical_ddl_test
    def test_create_index_replication(self):
        """Тест репликации создания индекса."""
        master = self.clusters['master']
        replica1 = self.clusters['replica1']

        schema_name = master['replication_schema']
        table_name = 'test_index_table'
        index_name = 'idx_test_index_table_data'

        # Создание таблицы на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        # Ожидание репликации создания таблицы
        time.sleep(2)

        # Создание индекса на мастере
        create_index_query = f"""
            CREATE INDEX {index_name} ON {schema_name}.{table_name} (data);
        """
        execute_sql(master['conn_params'], create_index_query, server_name=master['name'])

        # Ожидание репликации создания индекса
        time.sleep(2)

        # Проверка наличия индекса на replica2
        check_index_query = f"""
            SELECT indexname FROM pg_indexes
            WHERE schemaname = '{schema_name}' AND tablename = '{table_name}' AND indexname = '{index_name}';
        """
        results = execute_sql(replica1['conn_params'], check_index_query, server_name=replica1['name'], fetch=True)

        self.assertTrue(results, "Индекс не был создан на replica2")
        self.assertEqual(results[0][0], index_name, "Название индекса не совпадает на replica2")

        click.echo("Тест репликации создания индекса успешно пройден.")
