# factories/ddl_factory.py

from implementations.ddl_patch import DDLPatch
from implementations.vanilla import Vanilla
from interfaces.ddl_interface import DDLInterface

def get_ddl_implementation(db_type: str, implementation_type: str, conn_params: dict, server_name: str) -> DDLInterface:
	if db_type.lower() == "postgresql":
		if implementation_type == "ddl_patch":
			return DDLPatch(conn_params, server_name)
		elif implementation_type == "vanilla" :
			return Vanilla(conn_params, server_name)
		else:
			raise ValueError(f"Unsupported implementation type for PostgreSQL: {implementation_type}")
	else:
		raise ValueError(f"Unsupported DB type: {db_type}")
