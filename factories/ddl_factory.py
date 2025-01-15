# factories/ddl_factory.py

from interfaces.ddl_interface import DDLInterface
from implementations.vanilla import Vanilla
from implementations.ddl_patch import DDLPatch
from implementations.logical_ddl import LogicalDDLExt
from models.config import Config

def get_ddl_implementation(db_type: str, config: Config) -> DDLInterface:
	implementation_type = config.ddl_implementation
	if db_type.lower() == "postgresql":
		if implementation_type == "vanilla" :
			return Vanilla(config)
		elif implementation_type == "ddl_patch":
			return DDLPatch(config)
		elif implementation_type == "logicalddl_ext":
			return LogicalDDLExt(config)
		else:
			raise ValueError(f"Unsupported implementation type for PostgreSQL: {implementation_type}")
	else:
		raise ValueError(f"Unsupported DB type: {db_type}")
