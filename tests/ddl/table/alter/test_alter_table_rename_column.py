# tests/ddl/table/create/test_alter_table_alter_column_type.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestAlterTableAlterColumnType(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
        {'streaming': 'on'},
    ])
    def test_alter_table_alter_column_type(self):
        """Тестирование изменения типа данных колонки через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_alter_column_type'

        # Создаем таблицу на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                amount INTEGER
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Вставляем данные
        insert_data_query = f"""
            INSERT INTO {schema_name}.{table_name} (amount) VALUES (100);
        """
        execute_sql(master['conn_params'], insert_data_query, server_name=master['name'])

        time.sleep(1)

        # Изменяем тип данных колонки на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ALTER COLUMN amount TYPE BIGINT;
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем тип данных на реплике
        check_column_type_query = f"""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'amount';
        """
        column_type = execute_sql(replica['conn_params'], check_column_type_query, server_name=replica['name'], fetch=True)[0][0]
        self.assertEqual(column_type, 'bigint', "Тип данных колонки не был изменен на реплике")

        print(f"Изменение типа данных колонки было успешно реплицировано с опциями {self.current_subscription_options}.")
