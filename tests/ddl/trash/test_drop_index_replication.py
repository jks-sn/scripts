# tests/logical_ddl/ddl/test_drop_index_replication.py

from tests.base_test import BaseTest
import time
from utils.execute import execute_sql
from tests.test_tags import logical_ddl_test

class TestDropIndexReplication(BaseTest):
    @logical_ddl_test
    def test_drop_index_replication(self):
        """Тест репликации удаления индекса."""
        master = self.clusters['master']
        replica1 = self.clusters['replica1']
        
        schema_name = master['replication_schema']
        table_name = 'test_drop_index_table'
        index_name = 'idx_test_drop_index_table_data'
        
        # Создание таблицы и индекса на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
            CREATE INDEX {index_name} ON {schema_name}.{table_name} (data);
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])
        
        # Ожидание репликации создания таблицы и индекса
        time.sleep(2)
        
        # Удаление индекса на мастере
        drop_index_query = f"""
            DROP INDEX {schema_name}.{index_name};
        """
        execute_sql(master['conn_params'], drop_index_query, server_name=master['name'])
        
        # Ожидание репликации удаления индекса
        time.sleep(2)
        
        # Проверка отсутствия индекса на replica1
        check_index_query = f"""
            SELECT indexname FROM pg_indexes
            WHERE schemaname = '{schema_name}' AND tablename = '{table_name}' AND indexname = '{index_name}';
        """
        results = execute_sql(replica1['conn_params'], check_index_query, server_name=replica1['name'], fetch=True)
        
        self.assertFalse(results, "Индекс не был удален на replica1")
        
        print("Тест репликации удаления индекса успешно пройден.")
