# interfaces/ddl_interface.py

from abc import ABC, abstractmethod
from typing import List, Dict

class DDLInterface(ABC):

    @abstractmethod
    def create_publication(self, publication_name: str, schema_name: str, ddl: bool):
        pass

    @abstractmethod
    def drop_publication(self, publication_name: str):
        pass

    @abstractmethod
    def create_subscription(self, subscription_name: str, connection_info: str, publication_name: str):
        pass

    @abstractmethod
    def drop_subscription(self, subscription_name: str):
        pass

    @abstractmethod
    def create_schema(self, schema_name: str):
        pass

    @abstractmethod
    def drop_schema(self, schema_name: str):
        pass

    @abstractmethod
    def create_table(self, cluster_name: str, schema_name: str, table_name: str):
        pass

    @abstractmethod
    def drop_table(self, cluster_name: str, schema_name: str, table_name: str):
        pass

    @abstractmethod
    def add_column(self, cluster_name: str, schema_name: str, table_name: str,
                   column_name: str, column_type: str = "INTEGER", default_value=None):
        pass

    @abstractmethod
    def table_exists(self, cluster_name: str, schema_name: str, table_name: str) -> bool:
        pass

    @abstractmethod
    def get_table_columns(self, cluster_name: str, schema_name: str, table_name: str) -> List[Dict]:
        pass

