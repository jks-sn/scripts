# tests/ddl/table/create/test_alter_table_drop_column.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestAlterTableDropColumn(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'streaming': 'on'},
        {'copy_data': False},
        {'synchronous_commit': 'off'},
    ])
    def test_alter_table_drop_column(self):
        """Тестирование удаления колонки через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_drop_column'

        # Создаем таблицу с тремя колонками на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT,
                extra_column INTEGER
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем наличие колонок на реплике
        check_columns_query = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
        """
        columns_before = execute_sql(replica['conn_params'], check_columns_query, server_name=replica['name'], fetch=True)
        self.assertEqual(len(columns_before), 3, "Таблица не была корректно реплицирована на реплику")

        # Удаляем колонку на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            DROP COLUMN extra_column;
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что колонка была удалена на реплике
        columns_after = execute_sql(replica['conn_params'], check_columns_query, server_name=replica['name'], fetch=True)
        column_names = [col[0] for col in columns_after]
        self.assertNotIn('extra_column', column_names, "Колонка не была удалена на реплике")

        print(f"Удаление колонки было успешно реплицировано с опциями {self.current_subscription_options}.")
