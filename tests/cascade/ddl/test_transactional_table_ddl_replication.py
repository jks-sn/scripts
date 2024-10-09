# tests/cascade/ddl/test_transactional_table_ddl_replication.py

from tests.base_test import BaseTest
import time
import click
import psycopg2
from utils.execute import execute_sql
from tests.test_tags import ddl_test

class TestTransactionalTableDDLReplication(BaseTest):
    @ddl_test
    def test_transactional_table_ddl_replication(self):
        """Тест репликации транзакционных DDL операций с таблицами."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = 'test_transactional_ddl'

        # Начало транзакции на мастере
        conn_params = master['conn_params']
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            # Создание таблицы
            cursor.execute(f"""
                CREATE TABLE {schema_name}.{table_name} (
                    id SERIAL PRIMARY KEY
                );
            """)
            # Добавление столбца
            cursor.execute(f"""
                ALTER TABLE {schema_name}.{table_name}
                ADD COLUMN data TEXT;
            """)
            # Фиксация транзакции
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.fail(f"Ошибка при выполнении транзакции: {e}")
        finally:
            cursor.close()
            conn.close()
        
        # Ожидание репликации DDL
        time.sleep(2)
        
        # Проверка наличия таблицы и столбца на replica2
        check_column_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'data'
            );
        """
        results = execute_sql(replica2['conn_params'], check_column_query, server_name=replica2['name'], fetch=True)
        
        self.assertTrue(results[0][0], "Таблица или столбец не были созданы на replica2")
        
        click.echo("Тест репликации транзакционных DDL операций с таблицами успешно пройден.")

