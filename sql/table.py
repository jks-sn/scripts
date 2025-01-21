# utils/table.py

from jinja2 import Template

def generate_create_table_query(schema_name: str, table_name: str, columns_def: dict = None) -> str:
    if not columns_def:
        columns_def = {
            "id": "SERIAL PRIMARY KEY",
            "data": "TEXT"
        }

    columns_list = []
    for col_name, col_type in columns_def.items():
        columns_list.append(f"{col_name} {col_type}")
    columns_sql = ",\n        ".join(columns_list)

    template = Template("""
    CREATE TABLE IF NOT EXISTS {{ schema_name }}.{{ table_name }} (
        {{ columns_sql }}
    );
    """)
    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_sql=columns_sql
    )

def generate_drop_table_query(schema_name: str, table_name: str) -> str:
    """Creates an SQL query to drop a table."""
    template = Template("DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }} CASCADE;")
    return template.render(schema_name=schema_name, table_name=table_name)

def generate_add_column_query(schema_name: str, table_name: str, column_name: str, column_type: str, default_value=None) -> str:
    """
    Creates an SQL query to add a column to an existing table.
    If 'default_value' is provided, the column is created with that default.
    """
    if default_value is not None:
        template = Template("""
        ALTER TABLE {{ schema_name }}.{{ table_name }}
        ADD COLUMN {{ column_name }} {{ column_type }} DEFAULT '{{ default_value }}';
        """)
        return template.render(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
            default_value=default_value
        )
    else:
        template = Template("""
        ALTER TABLE {{ schema_name }}.{{ table_name }}
        ADD COLUMN {{ column_name }} {{ column_type }};
        """)
        return template.render(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=column_type
        )

def generate_rename_table_query(schema_name: str, old_table_name: str, new_table_name: str) -> str:
    """
    Creates an SQL query to rename a table in the same schema.
    """
    template = Template("""
    ALTER TABLE {{ schema_name }}.{{ old_table_name }}
    RENAME TO {{ new_table_name }};
    """)
    return template.render(
        schema_name=schema_name,
        old_table_name=old_table_name,
        new_table_name=new_table_name
    )

def generate_rename_column_query(schema_name: str, table_name: str, old_column_name: str, new_column_name: str) -> str:
    """
    Creates an SQL query to rename a column in a given table.
    """
    template = Template("""
    ALTER TABLE {{ schema_name }}.{{ table_name }}
    RENAME COLUMN {{ old_column_name }} TO {{ new_column_name }};
    """)
    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        old_column_name=old_column_name,
        new_column_name=new_column_name
    )

def generate_drop_column_query(schema_name: str, table_name: str, column_name: str) -> str:
    """
    Creates an SQL query to drop a column from a table.
    """
    template = Template("""
    ALTER TABLE {{ schema_name }}.{{ table_name }}
    DROP COLUMN {{ column_name }};
    """)
    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name
    )

def generate_alter_column_type_query(schema_name: str, table_name: str, column_name: str, new_type: str) -> str:
    """
    Creates an SQL query to change the type of an existing column.
    """
    template = Template("""
    ALTER TABLE {{ schema_name }}.{{ table_name }}
    ALTER COLUMN {{ column_name }} TYPE {{ new_type }};
    """)
    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        new_type=new_type
    )

def generate_insert_into_table_query(schema_name: str, table_name: str, data: dict) -> str:

    columns = list(data.keys())
    values = list(data.values())

    def sql_literal(value):
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

    columns_sql = ", ".join(columns)
    values_sql = ", ".join(sql_literal(v) for v in values)

    template = Template("""
    INSERT INTO {{ schema_name }}.{{ table_name }}
    ({{ columns_sql }})
    VALUES
    ({{ values_sql }});
    """)

    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_sql=columns_sql,
        values_sql=values_sql
    )
