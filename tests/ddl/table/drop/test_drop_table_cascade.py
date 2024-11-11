# tests/ddl/table/create/test_drop_table_cascade.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestDropTableCascade(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'enabled': True},
        {'streaming': 'on'},
        {'synchronous_commit': 'off'},
    ])
    def test_drop_table_cascade(self):
        """Тестирование удаления таблицы с зависимостями через DROP TABLE ... CASCADE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        parent_table = f'parent_table_{self.current_subscription_index}'
        child_table = f'child_table_{self.current_subscription_index}'

        # Создаем родительскую таблицу на мастере
        create_parent_table_query = f"""
            CREATE TABLE {schema_name}.{parent_table} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_parent_table_query, server_name=master['name'])

        # Создаем дочернюю таблицу с внешним ключом на мастере
        create_child_table_query = f"""
            CREATE TABLE {schema_name}.{child_table} (
                id SERIAL PRIMARY KEY,
                parent_id INTEGER REFERENCES {schema_name}.{parent_table}(id),
                info TEXT
            );
        """
        execute_sql(master['conn_params'], create_child_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что обе таблицы существуют на реплике
        parent_exists = check_table_exists(replica['conn_params'], schema_name, parent_table)
        child_exists = check_table_exists(replica['conn_params'], schema_name, child_table)
        self.assertTrue(parent_exists, "Родительская таблица не была реплицирована на реплику")
        self.assertTrue(child_exists, "Дочерняя таблица не была реплицирована на реплику")

        # Удаляем родительскую таблицу с CASCADE на мастере
        drop_table_query = f"""
            DROP TABLE {schema_name}.{parent_table} CASCADE;
        """
        execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что обе таблицы удалены на реплике
        parent_exists_after = check_table_exists(replica['conn_params'], schema_name, parent_table)
        child_exists_after = check_table_exists(replica['conn_params'], schema_name, child_table)
        self.assertFalse(parent_exists_after, "Родительская таблица не была удалена на реплике")
        self.assertFalse(child_exists_after, "Дочерняя таблица не была удалена на реплике")

        print(f"Удаление таблицы с CASCADE успешно реплицировано с опциями {self.current_subscription_options}.")
