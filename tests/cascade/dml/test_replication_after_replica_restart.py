# tests/cascade/dml/test_replication_after_replica_restart.py

from tests.base_test import BaseTest
import time
import click
from utils.execute import execute_sql
from commands.cluster import start_cluster, stop_cluster
from utils.log_handler import logger
from tests.test_tags import cascade_dml_test

class TestReplicationAfterReplicaRestart(BaseTest):
    @cascade_dml_test
    def test_replication_after_replica_restart(self):
        """Тест репликации данных после перезапуска replica2."""
        master = self.clusters['master']
        replica2 = self.clusters['replica2']

        schema_name = master['replication_schema']
        table_name = master['replication_table']
        replica2_name = replica2['name']

        # Остановка replica2
        logger.debug(f"Остановка {replica2_name}...")
        stop_cluster(replica2_name)

        # Вставка данных на мастере
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (data)
            VALUES ('Data after restart');
        """
        execute_sql(master['conn_params'], sql=insert_query, server_name=master['name'])

        # Запуск replica2
        logger.debug(f"Запуск {replica2_name}...")
        start_cluster(replica2_name)

        # Ожидание репликации
        time.sleep(2)

        # Проверка данных на replica2
        select_query = f"""
            SELECT data FROM {schema_name}.{table_name}
            WHERE data = 'Data after restart';
        """
        results = execute_sql(replica2['conn_params'], sql=select_query, server_name=replica2['name'], fetch=True)

        # Проверки
        self.assertIsNotNone(results, "Данные не реплицировались на replica2 после перезапуска")
        self.assertEqual(len(results), 1, "Количество строк не соответствует")
        self.assertEqual(results[0][0], 'Data after restart', "Данные не совпадают")

        click.echo("Тест репликации после перезапуска replica2 успешно пройден.")
