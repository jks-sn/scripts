# tests/ddl/table/create/test_create_table_with_constraints.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestCreateTableWithConstraints(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
        {'disable_on_error': 'true'},
    ])
    def test_create_table_with_constraints(self):
        """Тест репликации создания таблицы с ограничениями NOT NULL, CHECK и UNIQUE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_constraints'

        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER CHECK (age >= 18),
                email TEXT UNIQUE
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия ограничений на реплике
        check_constraints_query = f"""
            SELECT constraint_type, constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
        """
        results = execute_sql(replica['conn_params'], check_constraints_query, server_name=replica['name'], fetch=True)

        expected_constraints = [
            ('PRIMARY KEY', f'{table_name}_pkey'),
            ('UNIQUE', f'{table_name}_email_key'),
            ('CHECK', f'{table_name}_age_check'),
        ]

        constraint_types = [(row[0], row[1]) for row in results]

        for expected_type, expected_name in expected_constraints:
            self.assertIn((expected_type, expected_name), constraint_types, f"Ограничение {expected_name} не найдено на реплике")

        print(f"Тест репликации создания таблицы с ограничениями успешно пройден с опциями {self.current_subscription_options}.")
