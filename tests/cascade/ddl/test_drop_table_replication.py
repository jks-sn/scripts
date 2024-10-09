# tests/cascade/ddl/test_drop_table_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import ddl_test

class TestDropTableReplication(BaseTest):
    @ddl_test
    def test_drop_table_replication(self):
        """Тест репликации удаления таблицы."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']
        
        schema_name = master['replication_schema']
        table_name = 'test_drop_table'
        
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
        
        # Удаление таблицы на мастере
        drop_table_query = f"""
            DROP TABLE {schema_name}.{table_name};
        """
        execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])
        
        # Ожидание репликации DROP TABLE
        time.sleep(2)
        
        # Проверка отсутствия таблицы на replica2
        check_table_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            );
        """
        results = execute_sql(replica2['conn_params'], check_table_query, server_name=replica2['name'], fetch=True)
        
        self.assertFalse(results[0][0], "Таблица не была удалена на replica2")
        
        click.echo("Тест репликации удаления таблицы успешно пройден.")
