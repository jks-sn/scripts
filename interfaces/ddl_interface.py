# interfaces/ddl_interface.py

from abc import ABC, abstractmethod

class DDLInterface(ABC):
    @abstractmethod
    def create_schema(self, schema_name: str):
        pass

    @abstractmethod
    def drop_schema(self, schema_name: str):
        pass

    @abstractmethod
    def create_table(self, schema_name: str, table_name: str):
        pass

    @abstractmethod
    def drop_table(self, schema_name: str, table_name: str):
        pass

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