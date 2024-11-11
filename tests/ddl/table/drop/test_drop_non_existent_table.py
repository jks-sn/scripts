# tests/ddl/table/create/test_drop_non_existent_table.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestDropNonExistentTable(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'enabled': True},
        {'streaming': 'on'},
        {'synchronous_commit': 'off'},
    ])
    def test_drop_non_existent_table(self):
        """Тестирование удаления несуществующей таблицы через DROP TABLE IF EXISTS."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = f'non_existent_table_{self.current_subscription_index}'

        # Убеждаемся, что таблицы нет на мастере
        table_exists = check_table_exists(master['conn_params'], schema_name, table_name)
        self.assertFalse(table_exists, "Таблица уже существует на мастере, что не соответствует условиям теста")

        # Выполняем DROP TABLE IF EXISTS на мастере
        drop_table_query = f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name};
        """
        execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что на реплике команда не вызвала ошибок и изменений
        table_exists_on_replica = check_table_exists(replica['conn_params'], schema_name, table_name)
        self.assertFalse(table_exists_on_replica, "Таблица не должна существовать на реплике")

        print(f"Попытка удаления несуществующей таблицы успешно обработана с опциями {self.current_subscription_options}.")
