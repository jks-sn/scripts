# tests/cascade/ddl/ddl_table_replication/test_add_column_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import cascade_ddl_test

class TestAddColumnReplication(BaseTest):
    @cascade_ddl_test
    def test_add_column_replication(self):
        """Тест репликации добавления столбца в таблицу."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']
        
        schema_name = master['replication_schema']
        table_name = 'test_add_column'
        
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
        
        # Добавление нового столбца на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD COLUMN new_column TEXT;
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])
        
        # Ожидание репликации ALTER TABLE
        time.sleep(2)
        
        # Проверка наличия нового столбца на replica2
        check_column_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'new_column'
            );
        """
        results = execute_sql(replica2['conn_params'], check_column_query, server_name=replica2['name'], fetch=True)
        
        self.assertTrue(results[0][0], "Новый столбец не был добавлен на replica2")
        
        # Вставка данных с использованием нового столбца на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data, new_column)
            VALUES ('Test Data', 'New Column Data');
        """
        execute_sql(master['conn_params'], insert_query, server_name=master['name'])
        
        # Ожидание репликации данных
        time.sleep(2)
        
        # Проверка данных на replica2
        select_query = f"""
            SELECT data, new_column FROM {schema_name}.{table_name}
            WHERE data = 'Test Data';
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)
        
        self.assertEqual(results[0][0], 'Test Data', "Данные в столбце 'data' не совпадают")
        self.assertEqual(results[0][1], 'New Column Data', "Данные в новом столбце не совпадают")
        
        click.echo("Тест репликации добавления столбца успешно пройден.")
