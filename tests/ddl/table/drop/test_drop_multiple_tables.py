# tests/ddl/table/create/test_drop_multiple_tables.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time



class TestDropMultipleTables(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'streaming': 'on'},
        {'synchronous_commit': 'off'},
    ])
    def test_drop_multiple_tables(self):
        """Тестирование удаления нескольких таблиц в одной команде DROP TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_names = [f'multi_table_{i}_{self.current_subscription_index}' for i in range(1, 4)]

        # Создаем несколько таблиц на мастере
        for table_name in table_names:
            create_table_query = f"""
                CREATE TABLE {schema_name}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    data TEXT
                );
            """
            execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что таблицы существуют на реплике
        for table_name in table_names:
            table_exists = check_table_exists(replica['conn_params'], schema_name, table_name)
            self.assertTrue(table_exists, f"Таблица {table_name} не была реплицирована на реплику")

        # Удаляем все таблицы в одной команде
        drop_table_query = f"""
            DROP TABLE {', '.join([f'{schema_name}.{table_name}' for table_name in table_names])};
        """
        execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что все таблицы удалены на реплике
        for table_name in table_names:
            table_exists_after = check_table_exists(replica['conn_params'], schema_name, table_name)
            self.assertFalse(table_exists_after, f"Таблица {table_name} не была удалена на реплике")

        print(f"Удаление нескольких таблиц успешно реплицировано с опциями {self.current_subscription_options}.")
