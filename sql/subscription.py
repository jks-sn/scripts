# sql/subscription.py

from jinja2 import Template


def generate_create_subscription_query(subscription_name: str, connection_info: str, publication_name: str) -> str:
    """Creates an SQL query to create a subscription."""
    template = Template("CREATE SUBSCRIPTION {{ subscription_name }} CONNECTION '{{ connection_info }}' PUBLICATION {{ publication_name }};")
    return template.render(subscription_name=subscription_name, connection_info=connection_info, publication_name=publication_name)

def generate_drop_subscription_query(subscription_name: str) -> str:
    """Creates an SQL query to drop a subscription."""
    template = Template("DROP SUBSCRIPTION IF EXISTS {{ subscription_name }};")
    return template.render(subscription_name=subscription_name)