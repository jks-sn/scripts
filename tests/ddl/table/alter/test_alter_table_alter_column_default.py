# tests/ddl/table/create/test_alter_table_alter_column_default.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestAlterTableAlterColumnDefault(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без дополнительных опций
    ])
    def test_alter_table_alter_column_default(self):
        """Тестирование изменения значения по умолчанию через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_alter_column_default'

        # Создаем таблицу с значением по умолчанию на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                status TEXT DEFAULT 'pending'
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Изменяем значение по умолчанию на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ALTER COLUMN status SET DEFAULT 'approved';
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем новое значение по умолчанию на реплике
        check_default_query = f"""
            SELECT column_default FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'status';
        """
        default_value = execute_sql(replica['conn_params'], check_default_query, server_name=replica['name'], fetch=True)[0][0]
        self.assertIn('approved', default_value, "Значение по умолчанию не было изменено на реплике")

        print(f"Изменение значения по умолчанию было успешно реплицировано с опциями {self.current_subscription_options}.")
