# models/config.py

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

class ClusterConfig(BaseModel):
    name: str = Field(..., description="Cluster name")
    port: int = Field(default=5432, description="Cluster port used also for postgresql.conf")
    replication_schema: str = Field(default="replication", description="Schema used for replication")
    replication_table: str = Field(default="table1", description="Table used for replication")
    conn_params: ConnParams = Field(..., description="Parameters for connecting to this cluster")


class Config(BaseModel):
    pg_source_dir: str = Field(..., description="Directory containing PostgreSQL source code")
    pg_bin_dir: str = Field(..., description="Directory containing PostgreSQL binaries")
    pg_cluster_dir: str = Field(..., descrription="Dicrectory, that containing cluster")
    clusters: List[ClusterConfig] = Field(..., description="List of cluster configurations")

