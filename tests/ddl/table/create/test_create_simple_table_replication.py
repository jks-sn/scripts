# tests/ddl/table/create/test_create_simple_table_replication.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestCreateSimpleTableReplication(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
        {'synchronous_commit': 'local'},
        {'streaming': 'on'},
        {'streaming': 'parallel'},
        {'two_phase': 'true'},
        {'disable_on_error': 'true'},
        {'run_as_owner': 'true'},
        {'origin': 'none'},
        {'origin': 'any'},
        # Комбинации опций
        {'synchronous_commit': 'remote_apply', 'streaming': 'on'},
        {'two_phase': 'true', 'run_as_owner': 'true'},
    ])
    def test_create_simple_table_replication(self):
        """Тест репликации создания простой таблицы с различными опциями подписки."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_create_simple_table'

        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        check_table_query = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
        """
        results = execute_sql(replica['conn_params'], check_table_query, server_name=replica['name'], fetch=True)

        self.assertTrue(results, f"Таблица не была создана на реплике с опциями {self.current_subscription_options}")
        self.assertEqual(results[0][0], table_name, "Название таблицы не совпадает на реплике")

        print(f"Тест репликации создания простой таблицы успешно пройден с опциями {self.current_subscription_options}.")
