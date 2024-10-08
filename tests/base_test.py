# tests/main_test.py

import unittest
import click
from commands.clean_replication import clean_replication
from commands.replication import setup_replication
from utils.config_loader import get_clusters_dict
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
        setup_replication()
        pass

    def tearDown(self):
        """Очистка после каждого теста."""
        logger.debug("Очистка репликации после теста...")
        clean_replication()
        pass
