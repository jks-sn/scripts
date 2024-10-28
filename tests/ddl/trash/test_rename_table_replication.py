# tests/logical_ddl/ddl/test_rename_table_replication.py

from tests.base_test import BaseTest
import time
from utils.execute import execute_sql
from tests.test_tags import ddl_test

class TestRenameTableReplication(BaseTest):
    @ddl_test
    def test_rename_table_replication(self):
        """Тест репликации переименования таблицы."""
        master = self.clusters['master']
        replica1 = self.clusters['replica1']
        
        schema_name = master['replication_schema']
        old_table_name = 'test_rename_table'
        new_table_name = 'test_renamed_table'
        
        # Создание таблицы на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{old_table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])
        
        # Ожидание репликации создания таблицы
        time.sleep(2)
        
        # Переименование таблицы на мастере
        rename_table_query = f"""
            ALTER TABLE {schema_name}.{old_table_name}
            RENAME TO {new_table_name};
        """
        execute_sql(master['conn_params'], rename_table_query, server_name=master['name'])
        
        # Ожидание репликации переименования таблицы
        time.sleep(2)
        
        # Проверка наличия новой таблицы на replica1
        check_new_table_query = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{new_table_name}';
        """
        results_new = execute_sql(replica1['conn_params'], check_new_table_query, server_name=replica1['name'], fetch=True)
        
        # Проверка отсутствия старой таблицы на replica1
        check_old_table_query = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{old_table_name}';
        """
        results_old = execute_sql(replica1['conn_params'], check_old_table_query, server_name=replica1['name'], fetch=True)
        
        self.assertTrue(results_new, "Переименованная таблица не появилась на replica1")
        self.assertFalse(results_old, "Старая таблица все еще существует на replica1")
        
        print("Тест репликации переименования таблицы успешно пройден.")
