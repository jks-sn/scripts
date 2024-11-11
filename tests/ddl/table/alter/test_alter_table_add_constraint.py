# tests/ddl/table/create/test_alter_table_add_constraint.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestAlterTableAddConstraint(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без дополнительных опций
    ])
    def test_alter_table_add_constraint(self):
        """Тестирование добавления ограничения через ALTER TABLE."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_add_constraint'

        # Создаем таблицу без ограничений на мастере
        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id INTEGER,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Добавляем ограничение первичного ключа на мастере
        alter_table_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD CONSTRAINT pk_id PRIMARY KEY (id);
        """
        execute_sql(master['conn_params'], alter_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем наличие ограничения на реплике
        check_constraint_query = f"""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY';
        """
        constraints = execute_sql(replica['conn_params'], check_constraint_query, server_name=replica['name'], fetch=True)
        constraint_names = [c[0] for c in constraints]
        self.assertIn('pk_id', constraint_names, "Ограничение не было реплицировано на реплику")

        print(f"Добавление ограничения было успешно реплицировано с опциями {self.current_subscription_options}.")
