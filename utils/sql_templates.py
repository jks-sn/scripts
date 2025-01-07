# utils/sql_templates.py

from jinja2 import Template

def generate_create_schema_query(schema_name: str) -> str:
    """Creates an SQL query to create a schema."""
    template = Template("CREATE SCHEMA IF NOT EXISTS {{ schema_name }};")
    return template.render(schema_name=schema_name)

def generate_drop_schema_query(schema_name: str) -> str:
    """Creates an SQL query to drop a schema."""
    template = Template("DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;")
    return template.render(schema_name=schema_name)

def generate_create_table_query(schema_name: str, table_name: str) -> str:
    """Creates an SQL query to create a table."""
    template = Template("""
    CREATE TABLE IF NOT EXISTS {{ schema_name }}.{{ table_name }} (
        id SERIAL PRIMARY KEY,
        data TEXT
    );
    """)
    return template.render(schema_name=schema_name, table_name=table_name)

def generate_drop_table_query(schema_name: str, table_name: str) -> str:
    """Creates an SQL query to drop a table."""
    template = Template("DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }} CASCADE;")
    return template.render(schema_name=schema_name, table_name=table_name)

def generate_create_publication_query(publication_name: str, schema_name: str, ddl: bool) -> str:
    """Creates an SQL query to create a publication."""
    ddl_option = " WITH (ddl = 'table')" if ddl else ""
    template = Template("CREATE PUBLICATION {{ publication_name }} FOR TABLES IN SCHEMA {{ schema_name }}{{ ddl_option }};")
    return template.render(publication_name=publication_name, schema_name=schema_name, ddl_option=ddl_option)

def generate_drop_publication_query(publication_name: str) -> str:
    """Creates an SQL query to drop a publication."""
    template = Template("DROP PUBLICATION IF EXISTS {{ publication_name }};")
    return template.render(publication_name=publication_name)

def generate_create_subscription_query(subscription_name: str, connection_info: str, publication_name: str) -> str:
    """Creates an SQL query to create a subscription."""
    template = Template("CREATE SUBSCRIPTION {{ subscription_name }} CONNECTION '{{ connection_info }}' PUBLICATION {{ publication_name }};")
    return template.render(subscription_name=subscription_name, connection_info=connection_info, publication_name=publication_name)

def generate_drop_subscription_query(subscription_name: str) -> str:
    """Creates an SQL query to drop a subscription."""
    template = Template("DROP SUBSCRIPTION IF EXISTS {{ subscription_name }};")
    return template.render(subscription_name=subscription_name)