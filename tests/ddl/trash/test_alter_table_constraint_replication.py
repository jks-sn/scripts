# tests/logical_ddl/ddl/test_alter_table_constraint_replication.py

from tests.base_test import BaseTest
import time
from utils.execute import execute_sql
from tests.test_tags import ddl_test

class TestAlterTableConstraintReplication(BaseTest):
    @ddl_test
    def test_alter_table_constraint_replication(self):
        """Тест репликации изменения ограничения таблицы."""
        master = self.clusters['master']
        replica1 = self.clusters['replica1']
        
        schema_name = master['replication_schema']
        table_name = 'test_constraint_table'
        constraint_name = 'unique_data'
        
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
        
        # Добавление ограничения на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD CONSTRAINT {constraint_name} UNIQUE (data);
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])
        
        # Ожидание репликации изменения ограничения
        time.sleep(2)
        
        # Проверка наличия ограничения на replica1
        check_constraint_query = f"""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND constraint_type = 'UNIQUE';
        """
        results = execute_sql(replica1['conn_params'], check_constraint_query, server_name=replica1['name'], fetch=True)
        
        self.assertTrue(results, "Ограничение не было добавлено на replica1")
        self.assertEqual(results[0][0], constraint_name, "Название ограничения не совпадает на replica1")
        
        print("Тест репликации изменения ограничения таблицы успешно пройден.")
