#tests/tests.py
import subprocess
import sys
import click


def run_tests(tags=None):
    """
    Internal function that runs pytest with the given implementation and optional markers.
    """
    pytest_cmd = [
        "pytest",
        "tests",
        "-v"
    ]
    if tags:
        marker_expr = " or ".join(tags)
        pytest_cmd.extend(["-m", marker_expr])

    click.echo(f"[run_tests] Running: {' '.join(pytest_cmd)}")
    result = subprocess.run(pytest_cmd, check=False)
    sys.exit(result.returncode)

@click.command(name="tests")
@click.option("--tags", "-t", multiple=True, help="Markers (tags) to run (e.g. ddl, cascade_ddl)")
def tests_cmd(tags):
    """
    CLI command that runs pytest-based tests with the chosen DDL implementation.
    """
    run_tests(tags=tags)