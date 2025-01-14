# sql/schema.py

from jinja2 import Template


def generate_create_schema_query(schema_name: str) -> str:
    """Creates an SQL query to create a schema."""
    template = Template("CREATE SCHEMA IF NOT EXISTS {{ schema_name }};")
    return template.render(schema_name=schema_name)

def generate_drop_schema_query(schema_name: str) -> str:
    """Creates an SQL query to drop a schema."""
    template = Template("DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;")
    return template.render(schema_name=schema_name)