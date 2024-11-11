# tests/ddl/table/create/test_create_partitioned_table.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time


class TestCreatePartitionedTable(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},  # Без опций
    ])
    def test_create_partitioned_table(self):
        """Тест репликации создания разделенной таблицы с партициями."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        partitioned_table = 'orders'
        partition_table_1 = 'orders_p1'
        partition_table_2 = 'orders_p2'

        # Создаем разделенную таблицу
        create_partitioned_table_query = f"""
            CREATE TABLE {schema_name}.{partitioned_table} (
                order_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                order_date DATE
            ) PARTITION BY RANGE (order_date);
        """
        execute_sql(master['conn_params'], create_partitioned_table_query, server_name=master['name'])

        # Создаем партиции
        create_partition_1_query = f"""
            CREATE TABLE {schema_name}.{partition_table_1} PARTITION OF {schema_name}.{partitioned_table}
            FOR VALUES FROM ('2023-01-01') TO ('2023-06-30');
        """
        create_partition_2_query = f"""
            CREATE TABLE {schema_name}.{partition_table_2} PARTITION OF {schema_name}.{partitioned_table}
            FOR VALUES FROM ('2023-07-01') TO ('2023-12-31');
        """
        execute_sql(master['conn_params'], create_partition_1_query, server_name=master['name'])
        execute_sql(master['conn_params'], create_partition_2_query, server_name=master['name'])

        time.sleep(1)

        # Проверка наличия разделенной таблицы и партиций на реплике
        check_partitions_query = f"""
            SELECT inhrelid:test_create_table_with_like_clause
        """
        results = execute_sql(replica['conn_params'], check_partitions_query, server_name=replica['name'], fetch=True)

        expected_partitions = [
            (f'{schema_name}.{partition_table_1}', f'{schema_name}.{partitioned_table}'),
            (f'{schema_name}.{partition_table_2}', f'{schema_name}.{partitioned_table}'),
        ]

        self.assertEqual(len(results), 2, "Количество партиций на реплике не совпадает")

        for result, expected in zip(results, expected_partitions):
            self.assertEqual(result, expected, f"Партиция {expected[0]} не соответствует ожиданиям")

        print(f"Тест репликации создания разделенной таблицы с партициями успешно пройден с опциями {self.current_subscription_options}.")
