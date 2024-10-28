# tests/ddl/table/create/test_create_table_with_inheritance.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestCreateTableWithInheritance(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
    ])
    def test_create_table_with_inheritance(self):
        """Тест репликации создания таблицы с наследованием."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        parent_table = 'parent_table'
        child_table = 'child_table'

        # Создаем родительскую таблицу
        create_parent_table_query = f"""
            CREATE TABLE {schema_name}.{parent_table} (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
        """
        execute_sql(master['conn_params'], create_parent_table_query, server_name=master['name'])

        # Создаем дочернюю таблицу с наследованием
        create_child_table_query = f"""
            CREATE TABLE {schema_name}.{child_table} (
                age INTEGER
            ) INHERITS ({schema_name}.{parent_table});
        """
        execute_sql(master['conn_params'], create_child_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия дочерней таблицы на реплике
        check_child_table_query = f"""
            SELECT inhrelid::regclass::text AS child, inhparent::regclass::text AS parent
            FROM pg_inherits
            WHERE inhrelid = '{schema_name}.{child_table}'::regclass;
        """
        results = execute_sql(replica['conn_params'], check_child_table_query, server_name=replica['name'], fetch=True)

        self.assertTrue(results, "Дочерняя таблица не была создана на реплике или наследование не настроено")
        child, parent = results[0]
        self.assertEqual(child, f'{schema_name}.{child_table}', "Имя дочерней таблицы не совпадает")
        self.assertEqual(parent, f'{schema_name}.{parent_table}', "Имя родительской таблицы не совпадает")

        print(f"Тест репликации создания таблицы с наследованием успешно пройден с опциями {self.current_subscription_options}.")
