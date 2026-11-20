"""
Shared pytest configuration.

Tests marked `db` need the PostgreSQL database configured in .env. When it
is unreachable they are skipped with a clear reason instead of failing, so
the unit tests still run on a machine without Docker.
"""

from __future__ import annotations

import copy
from functools import lru_cache

import pytest


@lru_cache(maxsize=1)
def _database_available() -> bool:
    try:
        from src.database import check_connection
        return check_connection()
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    if _database_available():
        return
    skip = pytest.mark.skip(reason="PostgreSQL from .env is not reachable (docker compose up -d)")
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def settings():
    """A private copy of the settings that a test may modify freely."""

    from src.config import get_settings
    return copy.copy(get_settings())


@pytest.fixture
def isolated_settings(settings, tmp_path):
    """Settings whose data, log and lineage folders live under tmp_path."""

    for name in ("incoming_dir", "processed_dir", "rejected_dir", "archive_dir", "logs_dir"):
        path = tmp_path / name.removesuffix("_dir")
        path.mkdir()
        setattr(settings, name, path)
    settings.lineage_dir = tmp_path / "lineage"
    settings.project_root = tmp_path
    return settings
