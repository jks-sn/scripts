# tests/cascade/test_transaction_replication.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
import psycopg2

class TestTransactionReplication(BaseTest):
    def test_transaction_replication(self):
        """Тест репликации транзакции с несколькими операциями до replica2."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = master['replication_table']

        # Начало транзакции на мастере
        conn_params = master['conn_params']
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            # Вставка данных
            cursor.execute(f"""
                INSERT INTO {schema_name}.{table_name} (data)
                VALUES ('Transaction Data 1') RETURNING id;
            """)
            id1 = cursor.fetchone()[0]

            # Обновление данных
            cursor.execute(f"""
                UPDATE {schema_name}.{table_name}
                SET data = 'Transaction Data Updated'
                WHERE id = {id1};
            """)

            # Вставка ещё одной строки
            cursor.execute(f"""
                INSERT INTO {schema_name}.{table_name} (data)
                VALUES ('Transaction Data 2') RETURNING id;
            """)
            id2 = cursor.fetchone()[0]

            # Удаление первой строки
            cursor.execute(f"""
                DELETE FROM {schema_name}.{table_name}
                WHERE id = {id1};
            """)

            # Фиксация транзакции
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

        # Ожидание репликации
        time.sleep(2)

        # Проверка данных на replica2
        select_query = f"""
            SELECT id, data FROM {schema_name}.{table_name}
            WHERE id IN ({id1}, {id2});
        """
        results = execute_sql(replica2['conn_params'], select_query, server_name=replica2['name'], fetch=True)

        # Проверки
        self.assertIsNotNone(results, "Нет данных на replica2")
        # Ожидаем, что на replica2 будет только вторая строка
        self.assertEqual(len(results), 1, "Количество строк не соответствует ожидаемому")
        self.assertEqual(results[0][1], 'Transaction Data 2', "Данные не совпадают")

        click.echo("Тест репликации транзакции успешно пройден.")
