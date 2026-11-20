"""
Database connectivity for the healthcare claims pipeline.

Responsibilities:
1. Build a single shared SQLAlchemy engine from configuration.
2. Provide a transaction scope that commits on success and rolls back on error.
3. Apply the schema from sql/create_tables.sql.
4. Report basic connectivity for troubleshooting.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import get_settings


class DatabaseError(Exception):
    """Base exception for database errors."""

def get_engine() -> Engine:

    settings = get_settings()

    return create_engine(
        settings.database.sqlalchemy_url,
        pool_pre_ping=True,
        future=True,
    )


@contextmanager
def transaction() -> Iterator[Connection]:
    """
    Run a block of work inside one transaction.

    Everything the pipeline does to the database for a single run happens
    inside one of these blocks, so a failure at any stage leaves the database
    exactly as it was before the run started.
    """

    engine = get_engine()

    try:
        with engine.begin() as connection:
            yield connection

    except SQLAlchemyError as exc:
        raise DatabaseError(
            f"Transaction rolled back: {exc}"
        ) from exc


def check_connection() -> bool:

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))

        return True

    except SQLAlchemyError:
        return False


def init_database(
    schema_path: Path | None = None,
    indexes_path: Path | None = None,
) -> None:
    """
    Apply sql/create_tables.sql.

    The script is written with IF NOT EXISTS throughout, so running it against
    an existing database is a no-op. It does not migrate a database that was
    created from an older version of the script.
    """

    settings = get_settings()

    project_root = settings.project_root
    
    schema_path = schema_path or (
        project_root / "sql" / "create_tables.sql"
    )

    indexes_path = indexes_path or (
        project_root / "sql" / "indexes.sql"
    )

    if not schema_path.exists():
        raise DatabaseError(
            f"Schema script not found: {schema_path}"
        )

    if not indexes_path.exists():
        raise DatabaseError(
            f"Indexes script not found: {indexes_path}"
        )

    schema_script = schema_path.read_text(
        encoding="utf-8"
    )

    indexes_script = indexes_path.read_text(
        encoding="utf-8"
    )

    with transaction() as connection:

        # Create tables and constraints.
        connection.exec_driver_sql(schema_script)

        # Create indexes after the tables exist.
        connection.exec_driver_sql(indexes_script)

    print(
        f"Schema applied from {schema_path.name}"
    )

    print(
        f"Indexes applied from {indexes_path.name}"
    )


if __name__ == "__main__":

    settings = get_settings()

    print(
        f"Connecting to "
        f"{settings.database.host}:{settings.database.port}"
        f"/{settings.database.database}"
    )

    if not check_connection():
        raise SystemExit("Database is not reachable.")

    print("Connection successful.")

    init_database()
