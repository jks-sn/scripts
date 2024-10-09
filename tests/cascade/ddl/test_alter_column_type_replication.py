# tests/cascade/ddl/test_alter_column_type_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import ddl_test

class TestAlterColumnTypeReplication(BaseTest):
    @ddl_test
    def test_alter_column_type_replication(self):
        """Тест репликации изменения типа данных столбца."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']
        
        schema_name = master['replication_schema']
        table_name = 'test_alter_column_type'
        
        # Создание таблицы на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data INTEGER
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])
        
        # Ожидание репликации создания таблицы
        time.sleep(2)
        
        # Изменение типа данных столбца на мастере
        alter_column_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ALTER COLUMN data TYPE TEXT;
        """
        execute_sql(master['conn_params'], alter_column_query, server_name=master['name'])
        
        # Ожидание репликации ALTER TABLE
        time.sleep(2)
        
        # Проверка типа данных на replica2
        check_column_type_query = f"""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'data';
        """
        results = execute_sql(replica2['conn_params'], check_column_type_query, server_name=replica2['name'], fetch=True)
        
        self.assertEqual(results[0][0], 'text', "Тип данных столбца не совпадает на replica2")
        
        # Вставка данных с новым типом на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Text Data');
        """
        execute_sql(master['conn_params'], insert_query, server_name=master['name'])
        
        # Ожидание репликации данных
        time.sleep(2)
        
        # Проверка данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE data = 'Text Data';
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)
        
        self.assertEqual(results[0][0], 'Text Data', "Данные не совпадают на replica2")
        
        click.echo("Тест репликации изменения типа данных столбца успешно пройден.")
