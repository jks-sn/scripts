# tests/ddl/table/create/test_drop_table.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestDropTable(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'enabled': False},
        {'streaming': 'on'},
        {'copy_data': False},
        {'synchronous_commit': 'off'},
        {'slot_name': 'test_slot'},
    ])
    def test_drop_table(self):
        """Тестирование удаления существующей таблицы через DROP TABLE с различными subscription_options."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = f'test_drop_table_{self.current_subscription_index}'

        # Создаем таблицу на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что таблица существует на реплике (если подписка активна)
        if self.current_subscription_options.get('enabled', True):
            table_exists = check_table_exists(replica['conn_params'], schema_name, table_name)
            self.assertTrue(table_exists, "Таблица не была корректно реплицирована на реплику")
        else:
            # Если подписка отключена, таблица не должна существовать на реплике
            table_exists = check_table_exists(replica['conn_params'], schema_name, table_name)
            self.assertFalse(table_exists, "Таблица не должна существовать на реплике при отключенной подписке")
            print(f"Подписка отключена, тест пропущен для опций {self.current_subscription_options}.")
            return

        # Удаляем таблицу на мастере
        drop_table_query = f"""
            DROP TABLE {schema_name}.{table_name};
        """
        execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что таблица была удалена на реплике
        table_exists_on_replica = check_table_exists(replica['conn_params'], schema_name, table_name)
        if self.current_subscription_options.get('enabled', True):
            self.assertFalse(table_exists_on_replica, "Таблица не была удалена на реплике")
        else:
            # Если подписка была отключена, таблица должна остаться
            self.assertTrue(table_exists_on_replica, "Таблица не должна быть удалена на реплике при отключенной подписке")

        print(f"Удаление таблицы успешно реплицировано с опциями {self.current_subscription_options}.")
