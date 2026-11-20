"""
Database connectivity for the healthcare claims pipeline.

Responsibilities:
1. Build a single shared SQLAlchemy engine from configuration.
2. Provide a transaction scope that commits on success and rolls back on error.
3. Apply the schema: sql/create_tables.sql, then sql/migrations/*.sql in
   name order, then sql/indexes.sql.
4. Report basic connectivity for troubleshooting.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import get_settings


logger = logging.getLogger("healthcare_claims_pipeline.database")


class DatabaseError(Exception):
    """Base exception for database errors."""


@lru_cache(maxsize=4)
def _engine_for(url: str) -> Engine:
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        # Keep bound values (claim data) out of exception messages, which
        # end up in logs, the audit table and alert channels.
        hide_parameters=True,
    )


def get_engine() -> Engine:
    """
    Return the shared engine for the configured database.

    One engine per URL, so the connection pool is reused across calls
    instead of a new pool (and new connections) being built every time. The
    API and dashboard call this on every request.
    """

    return _engine_for(get_settings().database.sqlalchemy_url)


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
    migrations_dir: Path | None = None,
) -> None:
    """
    Apply the schema, migrations and indexes in one transaction.

    Every script is idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS), so
    running this against an existing database only adds what is missing.
    A dedicated migration tool such as Alembic would also track which
    migrations ran; at this project's size, idempotent scripts are enough.
    """

    settings = get_settings()

    project_root = settings.project_root
    
    schema_path = schema_path or (
        project_root / "sql" / "create_tables.sql"
    )

    indexes_path = indexes_path or (
        project_root / "sql" / "indexes.sql"
    )

    migrations_dir = migrations_dir or (
        project_root / "sql" / "migrations"
    )

    migrations = sorted(migrations_dir.glob("*.sql")) if migrations_dir.exists() else []

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

        # Evolve the schema, in name order.
        for migration in migrations:
            connection.exec_driver_sql(migration.read_text(encoding="utf-8"))

        # Create indexes after the tables exist.
        connection.exec_driver_sql(indexes_script)

    logger.info("Schema applied from %s", schema_path.name)

    for migration in migrations:
        logger.info("Migration applied: %s", migration.name)

    logger.info("Indexes applied from %s", indexes_path.name)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

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
