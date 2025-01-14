# models/config.py

import json
import os
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Optional


class ConnParams(BaseModel):
    host: str = Field(default="localhost", description="Host for connection")
    port: int = Field(default=5432, description="Port for connection")
    dbname: str = Field(default="postgres", description="Database name")
    user: str = Field(default="postgres", description="User name for connection")
    password: Optional[str] = Field(default="", description="Password for connection (can be empty)")

    @field_validator("password", mode="before")
    def password_is_optional(cls, value):
        """Sets the password to None if it's empty."""
        return value or ""

class Cluster(BaseModel):
    name: str = Field(..., description="Node name")
    role: str = Field(..., descriptiopn="Node role")
    port: int = Field(default=5432, description="Server port used also for postgresql.conf")
    replication_schema: str = Field(default="replication", description="Schema used for replication")
    replication_table: str = Field(default="table1", description="Table used for replication")
    conn_params: ConnParams = Field(..., description="Parameters for connecting to this node")


class Config(BaseModel):
    pg_source_dir: str = Field(..., description="Directory containing PostgreSQL source code")
    pg_bin_dir: str = Field(..., description="Directory containing PostgreSQL binaries")
    pg_cluster_dir: str = Field(..., descrription="Dicrectory, that containing cluster")
    nodes: List[Cluster] = Field(..., description="List of nodes configurations")

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