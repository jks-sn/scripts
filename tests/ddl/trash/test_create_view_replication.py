# tests/logical/ddl/test_create_view_replication.py

from tests.base_test import BaseTest
import time
from utils.execute import execute_sql
from tests.test_tags import logical_ddl_test

class TestCreateViewReplication(BaseTest):
    @logical_ddl_test
    def test_create_view_replication(self):
        """Тест репликации создания представления."""
        master = self.clusters['master']
        replica1 = self.clusters['replica1']
        
        schema_name = master['replication_schema']
        table_name = 'test_view_table'
        view_name = 'test_view'
        
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
        
        # Создание представления на мастере
        create_view_query = f"""
            CREATE VIEW {schema_name}.{view_name} AS
            SELECT id, data FROM {schema_name}.{table_name};
        """
        execute_sql(master['conn_params'], create_view_query, server_name=master['name'])
        
        # Ожидание репликации создания представления
        time.sleep(2)
        
        # Проверка наличия представления на replica1
        check_view_query = f"""
            SELECT table_name FROM information_schema.views
            WHERE table_schema = '{schema_name}' AND table_name = '{view_name}';
        """
        results = execute_sql(replica1['conn_params'], check_view_query, server_name=replica1['name'], fetch=True)
        
        self.assertTrue(results, "Представление не было создано на replica1")
        self.assertEqual(results[0][0], view_name, "Название представления не совпадает на replica1")
        
        print("Тест репликации создания представления успешно пройден.")
