# utils/execute.py
import json
from typing import Any, Dict, List, Optional
import psycopg2
import click
import subprocess
import sys
from utils.log_handler import logger

def execute_sql(
    conn_params: Dict[str, Any],
    sql: str,
    server_name: str,
    autocommit: bool = False,
    fetch: bool = False
) -> Optional[List[tuple]]:
    """
    Executes an SQL command on the specified PostgreSQL server.

    :param conn_params: Connection parameters for psycopg2.
    :param sql: The SQL query to execute.
    :param server_name: Name of the server (for logging purposes).
    :param autocommit: If True, the transaction is committed automatically.
    :param fetch: If True, fetches and returns the query results.
    :return: List of tuples containing the results if fetch is True, otherwise None.
    :raises: psycopg2.DatabaseError if a database error occurs.
    """
    try:
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = autocommit
            with conn.cursor() as cur:
                cur.execute(sql)
                results = cur.fetchall() if fetch else None
            if not autocommit:
                conn.commit()
            return results
    except psycopg2.DatabaseError as e:
        logger.error(f"Error executing SQL on server '{server_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error executing SQL on server '{server_name}': {e}")
        raise

def run_as_postgres(command: List[str], suppress_output: bool = True) -> None:
    """
    Executes a shell command as the 'postgres' user.

    :param command: List of command arguments to execute.
    :param suppress_output: If True, suppresses the command's stdout and stderr.
    :raises: subprocess.CalledProcessError if the command fails.
    """
    try:
        cmd = ['sudo', '-u', 'postgres', 'bash', '-c', ' '.join(command)]
        if suppress_output:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command as postgres: {' '.join(command)}")
        logger.error(str(e))
        click.secho(f"Error executing command as postgres: {' '.join(command)}", fg='red')
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error executing command as postgres: {e}")
        click.secho(f"Unexpected error executing command as postgres: {e}", fg='red')

