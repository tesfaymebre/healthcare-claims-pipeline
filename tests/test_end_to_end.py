"""
End-to-end runs against the real database.

Every run uses temporary data folders (the isolated_settings fixture), test
claims with a unique prefix, and explicit run ids; everything they write is
deleted afterwards, so the suite can run against a database holding real
pipeline output.
"""

import shutil
import uuid

import pandas as pd
import pytest
from sqlalchemy import text

from src.alerting import AlertManager
from src.database import get_engine, transaction
from src.lineage import trace_claim
from src.pipeline import run_pipeline
from tests.test_lineage import SOURCE_ONLY
from tests.test_validation import make_valid_claim

pytestmark = pytest.mark.db


@pytest.fixture
def prefix():
    return f"E2E{uuid.uuid4().hex[:8].upper()}_"


@pytest.fixture
def run_ids():
    ids: list[str] = []
    yield ids
    _cleanup(ids)


@pytest.fixture
def quiet_alerts(isolated_settings):
    # Alerts are still stored (and cleaned up), just not sent anywhere.
    return AlertManager(isolated_settings, notifiers=[], persist=True)


def _cleanup(ids, prefix=None):
    with transaction() as connection:
        for table in ("stg_healthcare_claims", "rejected_claims", "pipeline_audit",
                      "source_files", "pipeline_alerts", "fact_claims_history", "fact_claims"):
            connection.execute(text(f"DELETE FROM {table} WHERE pipeline_run_id = ANY(:ids)"), {"ids": ids})


def _new_run(run_ids):
    run_id = f"test-{uuid.uuid4().hex[:20]}"
    run_ids.append(run_id)
    return run_id


def claims(prefix, *overrides):
    rows = []
    for index, override in enumerate(overrides):
        row = {key: make_valid_claim()[key] for key in SOURCE_ONLY}
        row["claim_id"] = f"{prefix}{index}"
        row["patient_id"] = f"{prefix}PAT{index}"
        row.update(override)
        rows.append(row)
    return pd.DataFrame(rows)


def scalar(sql, **params):
    with get_engine().connect() as connection:
        return connection.execute(text(sql), params).scalar_one()


def test_run_rerun_force_and_update(isolated_settings, quiet_alerts, prefix, run_ids):
    incoming = isolated_settings.incoming_dir
    source = incoming.parent / "batch.csv"
    claims(
        prefix,
        {"claim_status": "Pending", "approved_amount": None, "paid_amount": None},
        {},
        {},
        {"claim_status": "Denied", "paid_amount": 0, "denial_reason": None},  # rejected
    ).to_csv(source, index=False)

    # 1. First load
    shutil.copy(source, incoming / "batch.csv")
    first = run_pipeline(_new_run(run_ids), settings=isolated_settings, alert_manager=quiet_alerts)

    assert first["run_status"] == "SUCCESS"
    assert first["load"]["inserted"] == 3
    assert first["records_rejected"] == 1
    assert first["reconciliation"]["passed"]
    assert not list(incoming.glob("*.csv"))
    assert (isolated_settings.archive_dir / first["run_id"] / "batch.csv").exists()
    assert (isolated_settings.processed_dir / first["run_id"] / "quality_summary.json").exists()

    # 2. Same bytes again: skipped by checksum, nothing reprocessed
    shutil.copy(source, incoming / "batch_copy.csv")
    second = run_pipeline(_new_run(run_ids), settings=isolated_settings, alert_manager=quiet_alerts)

    assert second["run_status"] == "SKIPPED"
    assert second["files_skipped"] == 1

    # 3. Forced reprocess: idempotent merge, no new history
    shutil.copy(source, incoming / "batch.csv")
    third = run_pipeline(_new_run(run_ids), force_reprocess=True, settings=isolated_settings,
                         alert_manager=quiet_alerts)

    assert third["load"]["inserted"] == 0
    assert third["load"]["unchanged"] == 3
    assert scalar("SELECT COUNT(*) FROM fact_claims WHERE claim_id LIKE :p", p=f"{prefix}%") == 3

    # 4. A real status change: one update, one new history row
    claims(prefix, {"claim_status": "Approved", "approved_amount": 450, "paid_amount": None,
                    "source_created_at": "2026-02-01T00:00:00"}).to_csv(incoming / "update.csv", index=False)
    fourth = run_pipeline(_new_run(run_ids), settings=isolated_settings, alert_manager=quiet_alerts)

    assert fourth["load"]["updated"] == 1

    with get_engine().connect() as connection:
        trace = trace_claim(connection, f"{prefix}0")

    assert [row["change_type"] for row in trace["history"]] == ["INSERT", "UPDATE"]
    assert [row["claim_status"] for row in trace["history"]] == ["Pending", "Approved"]
    assert trace["fact"]["source_filename"] == "update.csv"
    assert trace["fact"]["source_row_number"] == 1


def test_rejected_claim_keeps_its_raw_record_and_row(isolated_settings, quiet_alerts, prefix, run_ids):
    claims(prefix, {}, {"billed_amount": "abc"}).to_csv(isolated_settings.incoming_dir / "a.csv", index=False)

    stats = run_pipeline(_new_run(run_ids), settings=isolated_settings, alert_manager=quiet_alerts)

    with get_engine().connect() as connection:
        trace = trace_claim(connection, f"{prefix}1")

    rejection = trace["rejections"][0]
    assert rejection["rejection_reason"] == "INVALID_BILLED_AMOUNT"
    assert rejection["raw_record"]["billed_amount"] == "abc"
    assert rejection["source_row_number"] == 2
    assert rejection["pipeline_run_id"] == stats["run_id"]
    assert trace["fact"] is None


def test_database_failure_rolls_back_and_keeps_the_file(isolated_settings, quiet_alerts, prefix, run_ids):
    path = isolated_settings.incoming_dir / "bad.csv"
    claims(prefix, {}, {"claim_id": prefix + "X" * 80}).to_csv(path, index=False)
    run_id = _new_run(run_ids)

    with pytest.raises(Exception, match="value too long"):
        run_pipeline(run_id, settings=isolated_settings, alert_manager=quiet_alerts)

    assert path.exists()
    assert scalar("SELECT run_status FROM pipeline_audit WHERE pipeline_run_id = :r", r=run_id) == "FAILED"
    assert scalar("SELECT COUNT(*) FROM stg_healthcare_claims WHERE pipeline_run_id = :r", r=run_id) == 0
    assert scalar("SELECT COUNT(*) FROM fact_claims WHERE claim_id LIKE :p", p=f"{prefix}%") == 0
    assert scalar(
        "SELECT alert_code FROM pipeline_alerts WHERE pipeline_run_id = :r AND severity = 'CRITICAL'", r=run_id
    ) == "PIPELINE_FAILED"


def test_retry_with_same_run_id_replaces_the_attempt(isolated_settings, quiet_alerts, prefix, run_ids):
    path = isolated_settings.incoming_dir / "retry.csv"
    claims(prefix, {}, {"claim_id": prefix + "X" * 80}).to_csv(path, index=False)
    run_id = _new_run(run_ids)

    with pytest.raises(Exception):
        run_pipeline(run_id, settings=isolated_settings, alert_manager=quiet_alerts)

    # Fix the file and retry under the same id, as an orchestrator would.
    claims(prefix, {}, {}).to_csv(path, index=False)
    retried = run_pipeline(run_id, settings=isolated_settings, alert_manager=quiet_alerts)

    assert retried["run_status"] == "SUCCESS"
    assert scalar("SELECT COUNT(*) FROM stg_healthcare_claims WHERE pipeline_run_id = :r", r=run_id) == 2
    assert scalar("SELECT run_status FROM pipeline_audit WHERE pipeline_run_id = :r", r=run_id) == "SUCCESS"
