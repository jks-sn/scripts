#tests/ddl/table/create/test_create_table_with_various_types_replication.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time
import uuid


class TestCreateTableWithVariousDataTypes(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
        {'streaming': 'on'},
        {'two_phase': 'true'},
    ])
    def test_create_table_with_various_data_types(self):
        """Тест репликации создания таблицы с различными типами данных."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        table_name = 'test_various_data_types'

        create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                id UUID PRIMARY KEY,
                int_column INTEGER,
                text_column TEXT,
                bool_column BOOLEAN,
                date_column DATE,
                timestamp_column TIMESTAMP,
                json_column JSONB,
                array_column INTEGER[],
                composite_column POINT
            );
        """
        execute_sql(master['conn_params'], create_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия таблицы на реплике
        check_table_query = f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
        results = execute_sql(replica['conn_params'], check_table_query, server_name=replica['name'], fetch=True)

        expected_columns = [
            ('id', 'uuid'),
            ('int_column', 'integer'),
            ('text_column', 'text'),
            ('bool_column', 'boolean'),
            ('date_column', 'date'),
            ('timestamp_column', 'timestamp without time zone'),
            ('json_column', 'jsonb'),
            ('array_column', 'ARRAY'),
            ('composite_column', 'point'),
        ]

        self.assertEqual(len(results), len(expected_columns), "Количество колонок не совпадает")

        for (column_name, data_type), (expected_name, expected_type) in zip(results, expected_columns):
            self.assertEqual(column_name, expected_name, f"Ожидается колонка '{expected_name}', получена '{column_name}'")
            if expected_type == 'ARRAY':
                self.assertTrue('array' in data_type.lower(), f"Тип данных для '{column_name}' должен быть массивом")
            else:
                self.assertEqual(data_type, expected_type, f"Тип данных для '{column_name}' не совпадает")

        print(f"Тест репликации создания таблицы с различными типами данных успешно пройден с опциями {self.current_subscription_options}.")
