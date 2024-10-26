# tests/cascade/ddl/test_create_table_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from tests.test_tags import cascade_ddl_test

class TestCreateTableReplication(BaseTest):
    @cascade_ddl_test
    def test_create_table_replication(self):
        """Тест репликации создания таблицы в реплицируемой схеме."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']
        
        schema_name = master['replication_schema']
        table_name = 'test_create_table'
        
        # Создание таблицы на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])
        
        # Ожидание репликации DDL
        time.sleep(2)
        
        # Проверка наличия таблицы на replica2
        check_table_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            );
        """
        results = execute_sql(replica2['conn_params'], check_table_query, server_name=replica2['name'], fetch=True)
        
        # Проверки
        self.assertIsNotNone(results, "Не удалось проверить наличие таблицы на replica2")
        self.assertTrue(results[0][0], "Таблица не была создана на replica2")
        
        # Вставка данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Test Data');
        """
        execute_sql(master['conn_params'], insert_query, server_name=master['name'])
        
        # Ожидание репликации данных
        time.sleep(2)
        
        # Проверка данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE data = 'Test Data';
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)
        
        self.assertIsNotNone(results, "Нет данных на replica2")
        self.assertEqual(len(results), 1, "Количество строк не соответствует")
        self.assertEqual(results[0][0], 'Test Data', "Данные не совпадают")
        
        click.echo("Тест репликации создания таблицы успешно пройден.")
