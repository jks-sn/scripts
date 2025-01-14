# sql/publication.py

from jinja2 import Template


def generate_create_publication_query(publication_name: str, schema_name: str, ddl: bool) -> str:
    """Creates an SQL query to create a publication."""
    ddl_option = " WITH (ddl = 'table')" if ddl else ""
    template = Template("CREATE PUBLICATION {{ publication_name }} FOR TABLES IN SCHEMA {{ schema_name }}{{ ddl_option }};")
    return template.render(publication_name=publication_name, schema_name=schema_name, ddl_option=ddl_option)

def generate_drop_publication_query(publication_name: str) -> str:
    """Creates an SQL query to drop a publication."""
    template = Template("DROP PUBLICATION IF EXISTS {{ publication_name }};")
    return template.render(publication_name=publication_name)