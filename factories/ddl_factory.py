# factories/ddl_factory.py

from implementations.pgl_ddl_deploy import PG_DDL_Deploy
from implementations.pg_easy_replicate import PG_Easy_Replicate
from interfaces.ddl_interface import DDLInterface
from implementations.vanilla import Vanilla
from implementations.ddl_patch import DDLPatch
from implementations.logical_ddl import LogicalDDLExt
from models.config import Config

def get_ddl_implementation(db_type: str, config: Config) -> DDLInterface:
	impl = config.ddl_implementation or "vanilla"
	impl = impl.lower()
	if db_type.lower() == "postgresql":
		if impl == "vanilla" :
			return Vanilla(config)
		elif impl == "ddl_patch":
			return DDLPatch(config)
		elif impl == "logical_ddl":
			return LogicalDDLExt(config)
		elif impl == "pg_easy_replicate":
			return PG_Easy_Replicate(config)
		elif impl == "pg_ddl_deploy":
			return PG_DDL_Deploy(config)
		else:
			raise ValueError(f"Unsupported implementation type for PostgreSQL: {impl}")
	else:
		raise ValueError(f"Unsupported DB type: {db_type}")
