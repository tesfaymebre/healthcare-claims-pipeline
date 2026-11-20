"""
PostgreSQL loading module for the healthcare claims pipeline.

The load runs as three steps inside a single transaction:

1. Load validated records into stg_healthcare_claims.
2. Merge them into fact_claims, deriving the calculated fields on the way in.
3. Recompute duplicate_flag across all loaded history.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .config import Settings, get_settings
from .database import get_engine, transaction
from .ingestion import load_all_files
from .transformation import STAGING_COLUMNS, transform_claims
from .validation import print_validation_summary, validate_claims


class LoadingError(Exception):
    """Base exception for loading errors."""


STAGING_TABLE = "stg_healthcare_claims"


STAGING_TABLE_COLUMNS_SQL = """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = CURRENT_SCHEMA()
  AND table_name = :table_name
"""


DELETE_STAGING_SQL = """
DELETE FROM stg_healthcare_claims
WHERE pipeline_run_id = :run_id
"""


DELETE_REJECTED_SQL = """
DELETE FROM rejected_claims
WHERE pipeline_run_id = :run_id
"""


INSERT_STAGING_SQL = """
INSERT INTO stg_healthcare_claims (
    {columns}
) VALUES (
    {placeholders}
)
""".format(
    columns=",\n    ".join(STAGING_COLUMNS),
    placeholders=",\n    ".join(f":{column}" for column in STAGING_COLUMNS),
)


INSERT_REJECTED_SQL = """
INSERT INTO rejected_claims (
    claim_id,
    rejection_reason,
    raw_record,
    source_filename,
    pipeline_run_id
) VALUES (
    :claim_id,
    :rejection_reason,
    CAST(:raw_record AS JSONB),
    :source_filename,
    :pipeline_run_id
)
"""


COUNT_MERGE_CANDIDATES_SQL = """
SELECT COUNT(DISTINCT s.claim_id) AS candidates
FROM stg_healthcare_claims s
WHERE s.pipeline_run_id = :run_id
"""


# Note on the ON CONFLICT branch: billed_amount, the dates and the identifiers
# are treated as immutable for a given claim_id, so only the fields a payer can
# legitimately restate are refreshed. If a source ever did restate a billed
# amount, the CHECK constraints would abort the batch, which is the loud
# failure we want rather than a silently inconsistent row.
MERGE_FACT_SQL = """
INSERT INTO fact_claims (
    claim_id,
    patient_id,
    provider_id,
    service_date,
    submission_date,
    procedure_code,
    diagnosis_code,
    claim_type,
    billed_amount,
    approved_amount,
    paid_amount,
    unpaid_amount,
    claim_status,
    denial_reason,
    insurance_plan,
    processing_days,
    is_approved,
    is_denied,
    high_value_flag,
    late_submission_flag,
    duplicate_flag,
    source_filename,
    pipeline_run_id
)
SELECT
    s.claim_id,
    s.patient_id,
    s.provider_id,
    s.service_date,
    s.submission_date,
    s.procedure_code,
    s.diagnosis_code,
    s.claim_type,
    s.billed_amount,
    s.approved_amount,
    s.paid_amount,
    s.approved_amount - s.paid_amount,
    s.claim_status,
    s.denial_reason,
    s.insurance_plan,
    s.submission_date - s.service_date,
    s.claim_status IN ('Approved', 'Paid'),
    s.claim_status = 'Denied',
    s.billed_amount > CAST(:high_value_threshold AS NUMERIC(12,2)),
    (s.submission_date - s.service_date)
        > CAST(:late_submission_days AS INTEGER),
    -- Placeholder. Set by the recompute below, which is the only step that
    -- can see the whole history a duplicate is defined against.
    FALSE,
    s.source_filename,
    s.pipeline_run_id
FROM (
    SELECT DISTINCT ON (s.claim_id)
        s.*
    FROM stg_healthcare_claims s
    WHERE s.pipeline_run_id = :run_id
    ORDER BY
        s.claim_id,
        s.source_created_at DESC NULLS LAST,
        s.ctid DESC
) s
ON CONFLICT (claim_id) DO UPDATE
SET
    claim_status = EXCLUDED.claim_status,
    approved_amount = EXCLUDED.approved_amount,
    paid_amount = EXCLUDED.paid_amount,
    unpaid_amount = EXCLUDED.unpaid_amount,
    processing_days = EXCLUDED.processing_days,
    is_approved = EXCLUDED.is_approved,
    is_denied = EXCLUDED.is_denied,
    high_value_flag = EXCLUDED.high_value_flag,
    late_submission_flag = EXCLUDED.late_submission_flag,
    denial_reason = EXCLUDED.denial_reason,
    source_filename = EXCLUDED.source_filename,
    pipeline_run_id = EXCLUDED.pipeline_run_id,
    updated_at = CURRENT_TIMESTAMP
WHERE
    -- Skip rows that carry no new information, so rerunning the same file
    -- reports zero updates instead of rewriting every row and moving
    -- updated_at for no reason.
    fact_claims.claim_status IS DISTINCT FROM EXCLUDED.claim_status
    OR fact_claims.approved_amount IS DISTINCT FROM EXCLUDED.approved_amount
    OR fact_claims.paid_amount IS DISTINCT FROM EXCLUDED.paid_amount
    OR fact_claims.denial_reason IS DISTINCT FROM EXCLUDED.denial_reason
    OR fact_claims.high_value_flag IS DISTINCT FROM EXCLUDED.high_value_flag
    OR fact_claims.late_submission_flag IS DISTINCT FROM EXCLUDED.late_submission_flag
RETURNING (xmax::TEXT::BIGINT = 0) AS was_inserted
"""


# duplicate_flag describes a group of claims sharing a business key, and a
# claim arriving today can turn a claim loaded last month into a duplicate.
# A row-level upsert cannot express that, so the flag is recomputed after the
# merge. PARTITION BY treats NULL procedure codes as equal, which is the
# grouping we want and which a join on the key columns would silently miss.
#
# The window covers the whole table. 
RECOMPUTE_DUPLICATES_SQL = """
WITH affected_keys AS (
    SELECT DISTINCT
        patient_id,
        provider_id,
        service_date,
        procedure_code,
        billed_amount
    FROM stg_healthcare_claims
    WHERE pipeline_run_id = :run_id
),
affected_claims AS (
    SELECT
        f.claim_key,
        COUNT(*) OVER (
            PARTITION BY
                f.patient_id,
                f.provider_id,
                f.service_date,
                f.procedure_code,
                f.billed_amount
        ) > 1 AS is_duplicate
    FROM fact_claims f
    JOIN affected_keys k
      ON f.patient_id = k.patient_id
     AND f.provider_id = k.provider_id
     AND f.service_date = k.service_date
     AND f.procedure_code IS NOT DISTINCT FROM k.procedure_code
     AND f.billed_amount = k.billed_amount
)
UPDATE fact_claims f
SET
    duplicate_flag = a.is_duplicate,
    updated_at = CURRENT_TIMESTAMP
FROM affected_claims a
WHERE f.claim_key = a.claim_key
  AND f.duplicate_flag IS DISTINCT FROM a.is_duplicate;
"""


LOAD_SUMMARY_SQL = """
SELECT
    COUNT(*) AS fact_rows,
    COUNT(*) FILTER (WHERE duplicate_flag) AS duplicates,
    COUNT(*) FILTER (WHERE high_value_flag) AS high_value,
    COUNT(*) FILTER (WHERE late_submission_flag) AS late_submissions
FROM fact_claims
"""


class LoadResult:

    def __init__(
        self,
        run_id: str,
        staged: int = 0,
        rejected_by_validation: int = 0,
        merge_candidates: int = 0,
        inserted: int = 0,
        updated: int = 0,
        unchanged: int = 0,
        duplicate_flags_changed: int = 0,
    ) -> None:
        self.run_id = run_id
        self.staged = staged
        self.rejected_by_validation = rejected_by_validation
        self.merge_candidates = merge_candidates
        self.inserted = inserted
        self.updated = updated
        self.unchanged = unchanged
        self.duplicate_flags_changed = duplicate_flags_changed


def _to_json_safe(value: Any) -> Any:
    """
    Convert one pandas value into something json.dumps accepts.
    """

    if value is None or (
        not isinstance(value, (list, dict)) and pd.isna(value)
    ):
        return None

    if isinstance(value, (datetime, pd.Timestamp, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if hasattr(value, "item"):
        return value.item()

    return value


def load_staging(
    connection: Connection,
    staging_frame: pd.DataFrame,
    run_id: str,
    settings: Settings,
) -> int:
    """
    Replace this run's staging rows with the supplied frame.

    The delete is scoped to the run rather than truncating the table, so a
    retry of the same run starts clean while earlier runs stay available for
    reconciliation.
    """

    connection.execute(
        text(DELETE_STAGING_SQL),
        {"run_id": run_id},
    )

    if staging_frame.empty:
        return 0

    # The delete above and every step after it are scoped by run_id, so the
    # staged rows have to carry that same id. Trusting the frame's own column
    # would let a mismatch turn the merge into a silent no-op.
    staging_frame = staging_frame.assign(pipeline_run_id=run_id)

    records = staging_frame.to_dict(orient="records")

    statement = text(INSERT_STAGING_SQL)

    for start in range(0, len(records), settings.chunk_size):
        chunk = records[start : start + settings.chunk_size]
        connection.execute(statement, chunk)

    return len(records)


def load_rejected_records(
    connection: Connection,
    rejected_frame: pd.DataFrame,
    run_id: str,
    settings: Settings,
) -> int:
    """
    Store records rejected by validation, with the original values intact.

    Invalid records must never disappear, so the whole source row is preserved
    as JSON alongside every reason it failed.
    """

    connection.execute(
        text(DELETE_REJECTED_SQL),
        {"run_id": run_id},
    )

    if rejected_frame.empty:
        return 0

    payload_columns = [
        column
        for column in (
            settings.source_columns + settings.lineage_columns
        )
        if column in rejected_frame.columns
    ]

    records = []

    for row in rejected_frame.to_dict(orient="records"):

        raw_record = {
            column: _to_json_safe(row.get(column))
            for column in payload_columns
        }

        records.append(
            {
                "claim_id": _to_json_safe(row.get("claim_id")),
                "rejection_reason": row.get("rejection_reason")
                or "UNSPECIFIED",
                "raw_record": json.dumps(raw_record),
                "source_filename": _to_json_safe(
                    row.get("source_filename")
                ),
                "pipeline_run_id": run_id,
            }
        )

    statement = text(INSERT_REJECTED_SQL)

    for start in range(0, len(records), settings.chunk_size):
        chunk = records[start : start + settings.chunk_size]
        connection.execute(statement, chunk)

    return len(records)


def merge_fact(
    connection: Connection,
    run_id: str,
    settings: Settings,
) -> tuple[int, int, int]:
    """
    Upsert staging rows into fact_claims.

    Returns inserted, updated and unchanged counts. xmax is zero only on a
    freshly inserted row, which is how one statement can report both numbers.
    """

    candidates = connection.execute(
        text(COUNT_MERGE_CANDIDATES_SQL),
        {"run_id": run_id},
    ).scalar_one()

    rows = connection.execute(
        text(MERGE_FACT_SQL),
        {
            "run_id": run_id,
            "high_value_threshold": settings.high_value_threshold,
            "late_submission_days": settings.late_submission_days,
        },
    ).fetchall()

    inserted = sum(1 for row in rows if row.was_inserted)

    updated = len(rows) - inserted

    unchanged = candidates - len(rows)

    return inserted, updated, unchanged


def recompute_duplicate_flags(
    connection: Connection,
    run_id: str,
) -> int:
    """
    Recompute duplicate_flag across every claim loaded so far.
    """

    result = connection.execute(text(RECOMPUTE_DUPLICATES_SQL), {"run_id": run_id})

    return result.rowcount


def load_claims(
    staging_frame: pd.DataFrame,
    rejected_frame: pd.DataFrame,
    run_id: str,
    settings: Settings | None = None,
) -> LoadResult:
    """
    Run the whole load inside one transaction.

    Nothing is visible to a reader until every step has succeeded, so a failure
    in the merge cannot leave staging populated and fact half-written.
    """

    settings = settings or get_settings()

    result = LoadResult(
        run_id=run_id,
        rejected_by_validation=len(rejected_frame),
    )

    with transaction() as connection:

        result.staged = load_staging(
            connection,
            staging_frame,
            run_id,
            settings,
        )

        load_rejected_records(
            connection,
            rejected_frame,
            run_id,
            settings,
        )

        (
            result.inserted,
            result.updated,
            result.unchanged,
        ) = merge_fact(
            connection,
            run_id,
            settings,
        )

        result.merge_candidates = (
            result.inserted + result.updated + result.unchanged
        )

        result.duplicate_flags_changed = recompute_duplicate_flags(
            connection,
            run_id,
        )

    return result


def fetch_load_summary() -> dict[str, int]:
    """
    Read fact-table totals after a load.
    """

    with get_engine().connect() as connection:
        row = connection.execute(text(LOAD_SUMMARY_SQL)).one()

    return dict(row._mapping)


def print_load_summary(result: LoadResult) -> None:
    """
    Print a summary of one load.
    """

    print()
    print("=" * 60)
    print("LOAD SUMMARY")
    print("=" * 60)

    print(f"Run id:                  {result.run_id}")
    print(f"Rows staged:             {result.staged:,}")
    print(
        f"Rejected by validation:  "
        f"{result.rejected_by_validation:,}"
    )
    print(f"Merge candidates:        {result.merge_candidates:,}")
    print(f"  inserted:              {result.inserted:,}")
    print(f"  updated:               {result.updated:,}")
    print(f"  unchanged:             {result.unchanged:,}")
    print(
        f"Duplicate flags changed: "
        f"{result.duplicate_flags_changed:,}"
    )

    print("-" * 60)

    totals = fetch_load_summary()

    print(f"fact_claims rows:        {totals['fact_rows']:,}")
    print(f"  possible duplicates:   {totals['duplicates']:,}")
    print(f"  high value:            {totals['high_value']:,}")
    print(f"  late submissions:      {totals['late_submissions']:,}")

    print("=" * 60)


if __name__ == "__main__":



    frame = load_all_files()

    pipeline_run_id = str(frame["pipeline_run_id"].iloc[0])

    validation_result = validate_claims(frame)

    print_validation_summary(validation_result)

    staging_records = transform_claims(
        validation_result.valid_records
    )

    load_result = load_claims(
        staging_frame=staging_records,
        rejected_frame=validation_result.rejected_records,
        run_id=pipeline_run_id,
    )

    print_load_summary(load_result)
