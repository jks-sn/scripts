# tests/ddl/table/create/test_create_table_with_generated_column.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestCreateTableWithGeneratedColumn(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
    ])
    def test_create_table_with_generated_column(self):
        """Тест репликации создания таблицы с генерируемой колонкой."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_generated_column'

        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                length_cm INTEGER,
                length_m  NUMERIC GENERATED ALWAYS AS (length_cm / 100.0) STORED
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия генерируемой колонки на реплике
        check_generated_column_query = f"""
            SELECT column_name, is_generated, generation_expression
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'length_m';
        """
        results = execute_sql(replica['conn_params'], check_generated_column_query, server_name=replica['name'], fetch=True)

        self.assertTrue(results, "Генерируемая колонка не была создана на реплике")
        is_generated, generation_expression = results[0][1], results[0][2]
        self.assertEqual(is_generated, 'ALWAYS', "Колонка 'length_m' не является генерируемой на реплике")
        self.assertIn('length_cm / 100.0', generation_expression, "Выражение генерации колонки 'length_m' не совпадает")

        print(f"Тест репликации создания таблицы с генерируемой колонкой успешно пройден с опциями {self.current_subscription_options}.")
