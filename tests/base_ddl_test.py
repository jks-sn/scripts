# tests/ddl/base_ddl_test.py

import time
import unittest
from commands.clean_replication import clean_replication
from commands.replication import setup_master, setup_replica1
from commands.subscription import setup_subscription_with_options
from utils.execute import execute_sql
from utils.log_handler import logger
from utils.config_loader import get_clusters_dict

class BaseDDLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Настройка перед всеми DDL тестами."""
        logger.debug("setUpClass: Очистка репликации перед запуском DDL тестов...")
        clean_replication()
        cls.clusters = get_clusters_dict()
        cls.master = cls.clusters['master']
        cls.replica = cls.clusters['replica1']

    def setUp(self):
        """Настройка перед каждым DDL тестом."""
        logger.debug("Настройка мастера с DDL репликацией перед тестом...")
        setup_master(ddl=True)
        setup_replica1(ddl=True, logddl_tests=True);
        logger.debug("Мастер настроен для DDL репликации.")

    def tearDown(self):
        """Очистка после каждого DDL теста."""
        logger.debug("Очистка после теста...")
        clean_replication()

    def setup_subscription_with_options(self, options):
        """Настройка подписки с заданными опциями."""
        logger.debug(f"Настройка подписки с опциями: {options}")
        setup_subscription_with_options(options)

    def clean_subscription(self):
        """Очистка подписки после теста."""
        logger.debug("Очистка подписки...")
        clean_replication()

    def wait_for_subscription(self, subscription_name, timeout=1):
        """
        Ожидает, пока подписка станет активной.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            query = f"""
            SELECT pid FROM pg_stat_subscription
            WHERE subname = '{subscription_name}';
            """
        result = execute_sql(
            self.replica['conn_params'],
            query,
            server_name=self.replica['name'],
            fetch=True
        )
        if result:
            pid = result[0][0]
            if pid is not None:
                return
        time.sleep(1)
        raise TimeoutError(f"Подписка '{subscription_name}' не стала активной в течение {timeout} секунд.")
