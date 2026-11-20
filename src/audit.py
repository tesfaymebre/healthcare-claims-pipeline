"""
Pipeline audit and logging support.

- Generate pipeline run IDs.
- Configure application logging.
- Record pipeline execution status and row counts.
- Record failures for troubleshooting and auditability.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from config import Settings, get_settings
from database import transaction


LOGGER_NAME = "healthcare_claims_pipeline"

INSERT_AUDIT_SQL = """
INSERT INTO pipeline_audit (
    pipeline_run_id,
    pipeline_name,
    source_filename,
    run_start_time,
    run_end_time,
    records_received,
    records_loaded,
    records_rejected,
    records_flagged_duplicate,
    run_status,
    error_message
)
VALUES (
    :pipeline_run_id,
    :pipeline_name,
    :source_filename,
    :run_start_time,
    :run_end_time,
    :records_received,
    :records_loaded,
    :records_rejected,
    :records_flagged_duplicate,
    :run_status,
    :error_message
)
ON CONFLICT (pipeline_run_id) DO UPDATE
SET
    pipeline_name = EXCLUDED.pipeline_name,
    source_filename = EXCLUDED.source_filename,
    run_start_time = EXCLUDED.run_start_time,
    run_end_time = EXCLUDED.run_end_time,
    records_received = EXCLUDED.records_received,
    records_loaded = EXCLUDED.records_loaded,
    records_rejected = EXCLUDED.records_rejected,
    records_flagged_duplicate = EXCLUDED.records_flagged_duplicate,
    run_status = EXCLUDED.run_status,
    error_message = EXCLUDED.error_message
"""


def generate_run_id() -> str:
    """Generate a unique identifier for one pipeline execution."""

    return uuid.uuid4().hex


def configure_logging(
    settings: Settings | None = None,
) -> logging.Logger:
    """
    Configure application logging.

    Logs are written to both the console and the configured log file.
    """

    settings = settings or get_settings()

    logger = logging.getLogger(LOGGER_NAME)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, settings.log_level))

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    settings.logs_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_handler = logging.FileHandler(
        settings.logs_dir / "pipeline.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def record_audit(
    *,
    run_id: str,
    pipeline_name: str,
    source_filename: str | None,
    run_start_time: datetime,
    run_end_time: datetime | None,
    records_received: int,
    records_loaded: int,
    records_rejected: int,
    records_flagged_duplicate: int,
    run_status: str,
    error_message: str | None = None,
) -> None:
    """Insert or update one pipeline audit record."""

    with transaction() as connection:
        connection.execute(
            text(INSERT_AUDIT_SQL),
            {
                "pipeline_run_id": run_id,
                "pipeline_name": pipeline_name,
                "source_filename": source_filename,
                "run_start_time": run_start_time,
                "run_end_time": run_end_time,
                "records_received": records_received,
                "records_loaded": records_loaded,
                "records_rejected": records_rejected,
                "records_flagged_duplicate": records_flagged_duplicate,
                "run_status": run_status,
                "error_message": error_message,
            },
        )


def utc_now() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc)