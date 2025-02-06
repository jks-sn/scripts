# sql/user.py

from jinja2 import Template

def generate_create_user_query(username: str, password: str = '', superuser: bool = False) -> str:
    template = Template("""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{{ username }}') THEN
            CREATE ROLE {{ username }} WITH LOGIN PASSWORD '{{ password }}'
            {% if superuser %}
                SUPERUSER;
            {% else %}
                NOSUPERUSER;
            {% endif %}
        END IF;
    END
    $$;
    """)
    return template.render(username=username, password=password, superuser=superuser)


def generate_drop_user_query(username: str) -> str:
    template = Template("DROP ROLE IF EXISTS {{ username }};")
    return template.render(username=username)
