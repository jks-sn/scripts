# tests/ddl/table/create/test_create_table_with_like_clause.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time

class TestCreateTableWithLikeClause(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
    ])
    def test_create_table_with_like_clause(self):
        """Тест репликации создания таблицы с использованием LIKE и опций INCLUDING/EXCLUDING."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        original_table = 'original_table'
        new_table = 'new_table'

        # Создаем оригинальную таблицу
        create_original_table_query = f"""
            CREATE TABLE {schema_name}.{original_table} (
                id SERIAL PRIMARY KEY,
                data TEXT NOT NULL DEFAULT 'default data',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT check_data_length CHECK (length(data) <= 100)
            );
        """
        execute_sql(master['conn_params'], create_original_table_query, server_name=master['name'])

        # Создаем новую таблицу на основе оригинальной
        create_new_table_query = f"""
            CREATE TABLE {schema_name}.{new_table} (
                LIKE {schema_name}.{original_table} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS
            );
        """
        execute_sql(master['conn_params'], create_new_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия таблицы на реплике
        # Проверим, что дефолты скопировались, а ограничения нет
        check_columns_query = f"""
            SELECT column_name, column_default
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{new_table}'
            ORDER BY ordinal_position;
        """
        columns = execute_sql(replica['conn_params'], check_columns_query, server_name=replica['name'], fetch=True)

        # Проверяем наличие дефолтных значений
        defaults = dict(columns)
        self.assertIn('data', defaults, "Колонка 'data' отсутствует в новой таблице")
        self.assertIsNotNone(defaults['data'], "Дефолтное значение для 'data' не скопировано")

        # Проверяем отсутствие ограничений
        check_constraints_query = f"""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}' AND table_name = '{new_table}';
        """
        constraints = execute_sql(replica['conn_params'], check_constraints_query, server_name=replica['name'], fetch=True)
        self.assertFalse(constraints, "Ограничения не должны быть скопированы, но они есть на реплике")

        print(f"Тест репликации создания таблицы с LIKE успешно пройден с опциями {self.current_subscription_options}.")
