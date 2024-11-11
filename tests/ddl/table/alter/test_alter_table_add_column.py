# tests/ddl/table/create/test_alter_table_add_column.py

from commands.table import check_table_exists, get_table_columns
from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestAlterTableAddColumn(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'enabled': False},
        {'streaming': 'on'},
        {'copy_data': False},
        {'synchronous_commit': 'off'},
        {'streaming': 'on', 'synchronous_commit': 'off'},
    ])
    def test_alter_table_add_column(self):
        """Тестирование добавления колонки через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = f'test_add_column_{self.current_subscription_index}'

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
            columns = get_table_columns(replica['conn_params'], schema_name, table_name)
            self.assertEqual(len(columns), 2, "Таблица не была корректно реплицирована на реплику")
        else:
            # Если подписка отключена, таблица не должна существовать на реплике
            table_exists = check_table_exists(replica['conn_params'], schema_name, table_name)
            self.assertFalse(table_exists, "Таблица не должна существовать на реплике при отключенной подписке")
            print(f"Подписка отключена, тест пропущен для опций {self.current_subscription_options}.")
            return

        # Добавляем новую колонку на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD COLUMN new_column INTEGER DEFAULT 0;
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что новая колонка появилась на реплике
        columns = get_table_columns(replica['conn_params'], schema_name, table_name)
        column_names = [col['column_name'] for col in columns]
        self.assertIn('new_column', column_names, "Новая колонка не была реплицирована на реплику")

        print(f"Добавление колонки успешно реплицировано с опциями {self.current_subscription_options}.")
