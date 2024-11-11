# tests/ddl/table/create/test_alter_table_drop_constraint.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestAlterTableDropConstraint(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без дополнительных опций
    ])
    def test_alter_table_drop_constraint(self):
        """Тестирование удаления ограничения через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_drop_constraint'

        # Создаем таблицу с ограничением на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id INTEGER PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Удаляем ограничение на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            DROP CONSTRAINT {table_name}_pkey;
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем отсутствие ограничения на реплике
        check_constraint_query = f"""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY';
        """
        constraints = execute_sql(replica['conn_params'], check_constraint_query, server_name=replica['name'], fetch=True)
        self.assertFalse(constraints, "Ограничение не было удалено на реплике")

        print(f"Удаление ограничения было успешно реплицировано с опциями {self.current_subscription_options}.")
