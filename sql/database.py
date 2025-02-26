# sql/database.py

from jinja2 import Template

def generate_create_database_query(dbname: str, owner: str = None) -> str:
    """
    Generates an SQL command to create a database.

    :param dbname: Name of the database
    :param owner: (optional) Specify the role that will own the database
    """
    template_str = """
    CREATE DATABASE {{ dbname }}
    {% if owner %} OWNER {{ owner }}{% endif %}
    ;
    """
    t = Template(template_str)
    return t.render(dbname=dbname, owner=owner)


def generate_drop_database_query(dbname: str) -> str:
    """
    Generates an SQL command to drop a database.

    :param dbname: Name of the database to be dropped
    """
    template_str = "DROP DATABASE {{ dbname }};"
    t = Template(template_str)
    return t.render(dbname=dbname)
