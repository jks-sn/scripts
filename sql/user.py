# sql/user.py

from jinja2 import Template

def generate_create_user_query(username: str, password: str = '', superuser: bool = False) -> str:
    template = Template("""
    CREATE ROLE {{ username }} WITH LOGIN PASSWORD '{{ password }}'
    {% if superuser %}
        SUPERUSER;
    {% else %}
        NOSUPERUSER;
    {% endif %}
    """)
    return template.render(username=username, password=password, superuser=superuser)


def generate_drop_user_query(username: str) -> str:
    template = Template("DROP ROLE IF EXISTS {{ username }};")
    return template.render(username=username)
