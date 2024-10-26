# tests/main_test.py

import time
import unittest
import click
from commands.clean_replication import clean_replication
from commands.replication import setup_replication
from utils.config_loader import get_clusters_dict
from utils.execute import execute_sql
from utils.log_handler import logger

class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Настройка перед всеми тестами."""
        logger.debug("setUpClass: Очистка репликации перед запуском всех тестов...")
        clean_replication()
        cls.clusters = get_clusters_dict()

    @classmethod
    def tearDownClass(cls):
        """Дополнительная очистка после всех тестов (если необходимо)."""
        logger.debug("tearDownClass: Очистка после всех тестов...")


    def setUp(self):
        """Настройка перед каждым тестом."""
        logger.debug("Настройка репликации перед тестом...")

        test_method = getattr(self, self._testMethodName)
        test_tags = getattr(test_method, 'tags', [])
        ddl_required = 'cascade_ddl' in test_tags or 'logical_ddl' in test_tags
        setup_replication(ddl=ddl_required)
        logger.debug(f"Репликация настроена с DDL: {ddl_required}")
        pass

    def tearDown(self):
        """Очистка после каждого теста."""
        logger.debug("Очистка репликации после теста...")
        clean_replication()
        pass

    def wait_for_subscription(self, subscription_name, timeout=30):
        """
        Ожидает, пока подписка станет активной.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            query = f"""
                SELECT status FROM pg_stat_subscription
                WHERE subname = '{subscription_name}';
            """
            result = execute_sql(
                self.replica['conn_params'],
                query,
                server_name=self.replica['name'],
                fetch=True
            )
            if result and result[0][0] == 'streaming':
                return
            time.sleep(1)
        raise TimeoutError(f"Подписка '{subscription_name}' не стала активной в течение {timeout} секунд.")
