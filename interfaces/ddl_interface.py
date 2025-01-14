# interfaces/ddl_interface.py

from abc import ABC, abstractmethod

class DDLInterface(ABC):

    @abstractmethod
    def build_source(self, clean: bool = False) -> None:
        pass


    @abstractmethod
    def init_cluster(self) -> None:
        pass


    @abstractmethod
    def setup_master(self, node_name: str, ddl: bool) -> None:
        pass

    @abstractmethod
    def setup_replica(self, node_name: str, source_node_name: str, ddl: bool, cascade: bool) -> None:
        pass

    @abstractmethod
    def cleanup_cluster(self) -> None:
        pass


    @abstractmethod
    def start_cluster(self) -> None:
        pass

    @abstractmethod
    def status_cluster(self) -> None:
        pass

    @abstractmethod
    def stop_cluster(self) -> None:
        pass