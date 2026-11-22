"""Shared helpers for task-side database access and SQL file loading."""

import os
from pathlib import Path

import psycopg


DATABASE_URL = os.environ.get("DATABASE_URL")

# In docker-compose.yaml, the repository's top-level ./database folder is
# mounted into each Airflow container at /opt/airflow/database. This setting
# names that container path; it does not move the SQL files into airflow/.
SQL_DIRECTORY = Path(
    os.environ.get("SQL_DIRECTORY", "/opt/airflow/database")
)


def connect_to_project_database():
    """Open a connection to the project PostgreSQL database."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured for Airflow tasks.")
    return psycopg.connect(DATABASE_URL)


def read_sql_file(filename):
    """Read a SQL statement kept in the database/ directory."""
    sql_path = SQL_DIRECTORY / filename
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")
