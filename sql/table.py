# utils/table.py

from jinja2 import Template

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

def generate_add_column_query(schema_name: str, table_name: str, column_name: str, column_type: str, default_value=None) -> str:
    """Create SQL auery for add column"""
    if default_value is not None:
        template = Template("""
        ALTER TABLE {{ schema_name }}.{{ table_name }}
        ADD COLUMN {{ column_name }} {{ column_type }} DEFAULT '{{ default_value }}';
        """)
        return template.render(schema_name=schema_name, table_name=table_name, column_name=column_name, column_type=column_type, default_value=default_value)
    else:
        template = Template("""
        ALTER TABLE {{ schema_name }}.{{ table_name }}
        ADD COLUMN {{ column_name }} {{ column_type }};
        """)
        return template.render(schema_name=schema_name, table_name=table_name, column_name=column_name, column_type=column_type)
