# models/config.py

import json
import os
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Optional


class ConnParams(BaseModel):
    host: str = Field(default="localhost", json_schema_extra={"description": "Host for connection"})
    port: int = Field(default=5432, json_schema_extra={"description": "Port for connection"})
    dbname: str = Field(default="postgres", json_schema_extra={"description": "Database name"})
    user: str = Field(default="postgres", json_schema_extra={"description": "User name for connection"})
    password: Optional[str] = Field(default="", json_schema_extra={"description": "Password for connection (can be empty)"})

    @field_validator("password", mode="before")
    def password_is_optional(cls, value):
        """Sets the password to None if it's empty."""
        return value or ""

class Cluster(BaseModel):
    name: str = Field(..., json_schema_extra={"description": "Node name"})
    role: str = Field(..., json_schema_extra={"description": "Node role"})
    port: int = Field(default=5432, json_schema_extra={"description": "Server port used also for postgresql.conf"})
    replication_schema: str = Field(default="replication", json_schema_extra={"description": "Schema used for replication"})
    replication_table: str = Field(default="table1", json_schema_extra={"description": "Table used for replication"})
    conn_params: ConnParams = Field(..., json_schema_extra={"description": "Parameters for connecting to this node"})



class Config(BaseModel):
    ddl_implementation: Optional[str] = Field("vanilla", json_schema_extra={"description": "DDL implementation type"})
    pg_source_dir: str = Field(..., json_schema_extra={"description": "Directory containing PostgreSQL source code"})
    pg_bin_dir: str = Field(..., json_schema_extra={"description": "Directory containing PostgreSQL binaries"})
    pg_cluster_dir: str = Field(..., json_schema_extra={"description": "Directory containing clusters"})
    nodes: List[Cluster] = Field(..., json_schema_extra={"description": "List of nodes configurations"})


    def get_master_node(self) -> Cluster:
        masters = [n for n in self.nodes if n.role == "master"]
        if not masters:
            raise ValueError("No master cluster defined in configuration.")
        return masters[0]

    def get_subscriber_cluster(self) -> Cluster:
        subscribers = [n for n in self.nodes if n.role == "subscriber"]
        if not subscribers:
            raise ValueError("No subscriber cluster defined in configuration.")
        return subscribers[0]

    def get_cluster_by_name(self, name: str) -> Cluster:
        nodes = [n for n in self.nodes if n.name == name]
        if not nodes:
            raise ValueError(f"No cluster named '{name}' found in configuration.")
        return nodes[0]

    def get_replication_schema(self, cluster_name: str) -> str:
        cluster = self.get_cluster_by_name(cluster_name)
        return cluster.replication_schema

def load_config() -> Config:
    """Load configuration from config.json."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    try:
        with open(config_path) as f:
            config_data = json.load(f)

        return Config(**config_data)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error in the file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"An unknown error occurred while loading the configuration: {e}")

def get_nodes_dict() -> dict:
    """Transforme list of nodes to dict with name as key."""
    config = load_config()
    return {node.name: config.nodes.model_dump() for node in config.nodes}