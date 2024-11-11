# tests/ddl/table/create/test_drop_table_cascade.py

from tests.base_ddl_test import BaseDDLTest
from tests.test_tags import ddl_test
from utils.execute import execute_sql
from utils.subscription_decorator import subscription_options
import time



class TestDropTableRestrict(BaseDDLTest):
    @ddl_test
    @subscription_options([
        {},
    ])
    def test_drop_table_restrict(self):
        """Тестирование удаления таблицы с зависимостями через DROP TABLE ... RESTRICT."""
        master = self.master
        replica = self.replica

        schema_name = master['replication_schema']
        parent_table = f'parent_table_restrict_{self.current_subscription_index}'
        child_table = f'child_table_restrict_{self.current_subscription_index}'

        # Создаем родительскую таблицу на мастере
        create_parent_table_query = f"""
            CREATE TABLE {schema_name}.{parent_table} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """
        execute_sql(master['conn_params'], create_parent_table_query, server_name=master['name'])

        # Создаем дочернюю таблицу с внешним ключом на мастере
        create_child_table_query = f"""
            CREATE TABLE {schema_name}.{child_table} (
                id SERIAL PRIMARY KEY,
                parent_id INTEGER REFERENCES {schema_name}.{parent_table}(id),
                info TEXT
            );
        """
        execute_sql(master['conn_params'], create_child_table_query, server_name=master['name'])

        time.sleep(1)

        # Проверяем, что обе таблицы существуют на реплике
        parent_exists = check_table_exists(replica['conn_params'], schema_name, parent_table)
        child_exists = check_table_exists(replica['conn_params'], schema_name, child_table)
        self.assertTrue(parent_exists, "Родительская таблица не была реплицирована на реплику")
        self.assertTrue(child_exists, "Дочерняя таблица не была реплицирована на реплику")

        # Пытаемся удалить родительскую таблицу с RESTRICT на мастере
        drop_table_query = f"""
            DROP TABLE {schema_name}.{parent_table} RESTRICT;
        """
        try:
            execute_sql(master['conn_params'], drop_table_query, server_name=master['name'])
        except Exception as e:
            # Ожидаем ошибку об имеющихся зависимостях
            self.assertIn('cannot drop table', str(e))
            self.assertIn('because other objects depend on it', str(e))
        else:
            self.fail("Ожидалась ошибка при попытке удаления таблицы с зависимостями и RESTRICT")

        # Убеждаемся, что таблицы все еще существуют на реплике
        parent_exists_after = check_table_exists(replica['conn_params'], schema_name, parent_table)
        child_exists_after = check_table_exists(replica['conn_params'], schema_name, child_table)
        self.assertTrue(parent_exists_after, "Родительская таблица не должна быть удалена на реплике")
        self.assertTrue(child_exists_after, "Дочерняя таблица не должна быть удалена на реплике")

        print(f"Попытка удаления таблицы с RESTRICT корректно обработана с опциями {self.current_subscription_options}.")
