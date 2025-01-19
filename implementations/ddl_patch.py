# implementations/ddl_patch.py

import os
import subprocess
import sys
from implementations.base_ddl import BaseDDL
from sql.publication import generate_drop_publication_query
from sql.schema import generate_drop_schema_query
from sql.subscription import generate_drop_subscription_query
from utils.execute import execute_sql
from utils.log_handler import logger

class DDLPatch(BaseDDL):
    """
    :param config: Pydantic-model (Config), where config.clusters — list of clusters
    """
    LOG_TAG = "[DDLPatch]"

    def build_source(self, clean: bool = False) -> None:
        logger.debug("{self.LOG_TAG} Building PostgreSQL from source if needed.")
        super().build_source(clean=clean)
        logger.debug("{self.LOG_TAG} PostgreSQL built and installed successfully.")

    def init_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Init cluster starting...")
        super().init_cluster()
        logger.debug(f"{self.LOG_TAG} cluster have been initialized successfully.")

    def setup_master(self, node_name: str, ddl: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with ddl={ddl}...")
        super().setup_master(node_name=node_name, ddl=True)
        logger.debug(f"{self.LOG_TAG} Setting up master '{node_name}' with ddl={ddl} finish successfully")

    def setup_replica(self, node_name: str, master_node_name: str, ddl: bool, cascade: bool) -> None:
        logger.debug(f"{self.LOG_TAG} Settuing up replica '{node_name} with master '{master_node_name}'.")
        super().setup_replica(node_name=node_name, master_node_name=master_node_name, ddl=True, cascade=cascade)
        logger.debug(f"{self.LOG_TAG} Setting up replica '{node_name}' with ddl={True} finish successfully")

    def cleanup_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Cleaning up cluster.")
        super().cleanup_cluster()
        logger.debug("Replication cleanup completed on all nodes.")

    def start_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Starting all cluster nodes.")
        super().start_cluster()
        logger.debug(f"{self.LOG_TAG} All cluster nodes have been started successfully.")

    def status_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Checking status of all cluster nodes.")
        super().status_cluster()
        logger.debug(f"{self.LOG_TAG} Status check completed for all cluster nodes.")

    def stop_cluster(self) -> None:
        logger.debug(f"{self.LOG_TAG} Stopping all cluster nodes.")
        super().stop_cluster()
        logger.debug(f"{self.LOG_TAG} All cluster nodes have been stopped successfully.")