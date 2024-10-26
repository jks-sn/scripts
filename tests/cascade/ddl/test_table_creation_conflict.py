# tests/cascade/ddl/test_table_creation_conflict.py

from tests.base_test import BaseTest
import time
from utils.execute import execute_sql
from tests.test_tags import cascade_ddl_test

class TestTableCreationConflict(BaseTest):
    @cascade_ddl_test
    def test_table_creation_conflict(self):
        """Тест обработки конфликта при создании таблицы."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']
        
        schema_name = master['replication_schema']
        table_name = 'test_conflict_table'
        
        # Создание таблицы на replica2
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY
            );
        """
        execute_sql(replica2['conn_params'], create_table_query, server_name=replica2['name'])
        
        # Создание таблицы на мастере
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])
        
        # Ожидание репликации DDL
        time.sleep(2)
        
        # Проверка, что репликация продолжает работать
        # Вставка данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (id)
            VALUES (1);
        """
        execute_sql(master['conn_params'], insert_query, server_name=master['name'])
        
        # Ожидание репликации данных
        time.sleep(2)
        
        # Проверка данных на replica2
        select_query = f"""
            SELECT id FROM {schema_name}.{table_name}
            WHERE id = 1;
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)
        
        self.assertEqual(len(results), 1, "Данные не реплицировались на replica2 после конфликта")
        self.assertEqual(results[0][0], 1, "Данные не совпадают на replica2")
        
        print("Тест обработки конфликта при создании таблицы успешно пройден.")
