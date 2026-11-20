"""
End-to-end pipeline orchestration.

A run is a sequence of stages. Each stage takes and returns plain,
JSON-serializable dicts, so the same functions back two orchestrators:

- run_pipeline() / `python -m src.pipeline` runs every stage in one process.
- dags/healthcare_claims_dag.py runs each stage as its own Airflow task,
  passing the dicts through XCom.

    start_run        discover files, fingerprint them, skip already-loaded
                     content, write audit RUNNING, emit lineage START
    process_run      ingest -> validate -> transform -> load (one transaction),
                     write the quality summary
    reconcile_run    check the counts agree, in memory and in the database
    alert_on_quality raise threshold alerts for a run that succeeded
    archive_run      move files out of incoming, only after the commit
    finish_run       write the final audit row, emit lineage COMPLETE
    fail_run         audit FAILED, lineage FAIL, failure alert

Retrying a stage with the same run id is safe: the loader replaces the run's
staging and rejected rows, the fact merge is an idempotent upsert, and the
audit row is upserted by run id.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import uuid
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from .alerting import AlertManager, evaluate_quality_alerts, failure_alert
from .audit import configure_logging, generate_run_id, record_audit, recent_volumes, utc_now
from .config import Settings, get_settings
from .database import get_engine, transaction
from .ingestion import (
    FILE_ACCEPTED,
    FILE_ARCHIVED,
    FILE_LOADED,
    FILE_REJECTED,
    FILE_SKIPPED,
    FileDiscoveryError,
    IngestionError,
    SourceFile,
    describe_file,
    discover_csv_files,
    ingest_files,
)
from .lineage import LineageEmitter, previously_loaded, record_source_files
from .loading import load_claims
from .rules import describe_rules, get_rule_set
from .transformation import transform_claims
from .validation import validate_claims


logger = logging.getLogger("healthcare_claims_pipeline.pipeline")


class ReconciliationError(Exception):
    """Raised when a run's counts do not agree."""


class PipelineLockedError(Exception):
    """Raised when another run already holds the pipeline lock."""


# Helpers

def _files(context: dict[str, Any]) -> list[SourceFile]:
    return [SourceFile.from_dict(item) for item in context["files"]]


def _count(files: list[SourceFile], *statuses: str) -> int:
    return sum(1 for file in files if file.status in statuses)


def deterministic_run_id(*parts: str) -> str:
    """
    A run id derived from its inputs, e.g. the Airflow dag id and run id.

    An orchestrator retrying or clearing a task gets the same id back, which
    is what makes the retry replace the earlier attempt instead of adding a
    second copy beside it.
    """

    return uuid.uuid5(uuid.NAMESPACE_URL, "healthcare-claims/" + "/".join(parts)).hex


@contextmanager
def run_lock(settings: Settings) -> Iterator[None]:
    """
    Hold a Postgres advisory lock for the duration of a CLI run.

    Two overlapping runs would read the same incoming files. Airflow prevents
    that with max_active_runs=1; this covers a cron job or a second terminal.
    """

    key = zlib.crc32(settings.pipeline_name.encode("utf-8"))

    with get_engine().connect() as connection:

        acquired = connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar_one()

        if not acquired:
            raise PipelineLockedError("Another pipeline run is in progress.")

        try:
            yield
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
            connection.commit()


# Stages

def start_run(
    run_id: str | None = None,
    *,
    force_reprocess: bool = False,
    settings: Settings | None = None,
    incoming_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Open a run: pick the files it owns and record that it started.

    Raises FileDiscoveryError when the incoming directory has no CSV files.
    """

    settings = settings or get_settings()

    run_id = run_id or generate_run_id()

    started_at = utc_now()

    files = [describe_file(path) for path in discover_csv_files(incoming_dir or settings.incoming_dir)]

    # The same bytes twice in one drop (e.g. a copy with a new name).
    seen: dict[str, str] = {}
    for file in files:
        if file.sha256 in seen:
            file.status = FILE_SKIPPED
            file.error = f"Same content as {seen[file.sha256]} in this run"
        else:
            seen[file.sha256] = file.filename

    if settings.skip_previously_loaded_files and not force_reprocess:
        with get_engine().connect() as connection:
            loaded = previously_loaded(
                connection,
                [file.sha256 for file in files if file.status == FILE_ACCEPTED],
            )
        for file in files:
            if file.status == FILE_ACCEPTED and file.sha256 in loaded:
                earlier = loaded[file.sha256]
                file.status = FILE_SKIPPED
                file.error = (
                    f"Already loaded as {earlier['filename']} "
                    f"by run {earlier['pipeline_run_id']}"
                )

    for file in files:
        if file.status == FILE_SKIPPED:
            logger.warning("Skipping %s: %s", file.filename, file.error)

    record_audit(
        run_id=run_id,
        pipeline_name=settings.pipeline_name,
        source_filename=", ".join(file.filename for file in files)[:255],
        run_start_time=started_at,
        run_end_time=None,
        records_received=None,
        records_loaded=None,
        records_rejected=None,
        records_flagged_duplicate=None,
        files_received=len(files),
        files_skipped=_count(files, FILE_SKIPPED),
        run_status="RUNNING",
    )

    with transaction() as connection:
        record_source_files(connection, run_id, files)

    context = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "force_reprocess": force_reprocess,
        "files": [file.to_dict() for file in files],
    }

    LineageEmitter(settings).emit(
        LineageEmitter(settings).build_event(
            "START",
            run_id,
            [file for file in files if file.status == FILE_ACCEPTED],
        )
    )

    logger.info(
        "Run %s started: %d file(s), %d to process, %d skipped",
        run_id,
        len(files),
        _count(files, FILE_ACCEPTED),
        _count(files, FILE_SKIPPED),
    )

    return context


def process_run(
    context: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    """
    Ingest, validate, transform and load the run's files.

    Everything written to the claims tables happens in one transaction inside
    load_claims(); if it fails, nothing from this run is visible.
    """

    settings = settings or get_settings()

    run_id = context["run_id"]

    files = _files(context)

    stats: dict[str, Any] = {
        "run_id": run_id,
        "records_received": 0,
        "records_valid": 0,
        "records_rejected": 0,
        "records_flagged_duplicate": 0,
        "records_with_warnings": 0,
        "rule_hits": {},
        "warning_hits": {},
        "load": None,
    }

    if not _count(files, FILE_ACCEPTED):
        logger.info("Run %s: nothing to process, every file was skipped", run_id)
        return _with_file_stats(stats, files)

    try:
        ingestion = ingest_files(files, run_id, settings)
    except IngestionError:
        # Record which files were rejected and why before failing the run.
        with transaction() as connection:
            record_source_files(connection, run_id, files)
        raise

    validation = validate_claims(ingestion.dataframe, settings)

    staging = transform_claims(validation.valid_records)

    load = load_claims(
        staging_frame=staging,
        rejected_frame=validation.rejected_records,
        run_id=run_id,
        settings=settings,
        source_files=files,
    )

    for file in files:
        if file.status == FILE_ACCEPTED:
            file.status = FILE_LOADED

    summary = validation.summary

    stats.update(
        {
            "records_received": summary["records_received"],
            "records_valid": summary["records_valid"],
            "records_rejected": summary["records_rejected"],
            "records_flagged_duplicate": summary["records_flagged_duplicate"],
            "records_with_warnings": summary["records_with_warnings"],
            "rule_hits": summary["rule_hits"],
            "warning_hits": summary["warning_hits"],
            "by_file": summary["by_file"],
            "rules_version": summary["rules_version"],
            "load": load.to_dict(),
        }
    )

    stats = _with_file_stats(stats, files)

    write_quality_summary(stats, settings)

    logger.info(
        "Run %s loaded: received=%s valid=%s rejected=%s inserted=%s updated=%s unchanged=%s",
        run_id,
        f"{stats['records_received']:,}",
        f"{stats['records_valid']:,}",
        f"{stats['records_rejected']:,}",
        f"{load.inserted:,}",
        f"{load.updated:,}",
        f"{load.unchanged:,}",
    )

    return stats


def _with_file_stats(stats: dict[str, Any], files: list[SourceFile]) -> dict[str, Any]:
    stats["files"] = [file.to_dict() for file in files]
    stats["files_received"] = len(files)
    stats["files_loaded"] = _count(files, FILE_LOADED, FILE_ARCHIVED)
    stats["files_rejected"] = _count(files, FILE_REJECTED)
    stats["files_skipped"] = _count(files, FILE_SKIPPED)
    stats["rejected_files"] = [file.filename for file in files if file.status == FILE_REJECTED]
    return stats


def write_quality_summary(stats: dict[str, Any], settings: Settings) -> Path:
    """Write the run's data-quality summary next to the processed data."""

    directory = settings.processed_dir / stats["run_id"]
    directory.mkdir(parents=True, exist_ok=True)

    path = directory / "quality_summary.json"

    report = {
        "run_id": stats["run_id"],
        "generated_at": utc_now().isoformat(),
        "records": {
            key: stats[key]
            for key in (
                "records_received",
                "records_valid",
                "records_rejected",
                "records_flagged_duplicate",
                "records_with_warnings",
            )
        },
        "rule_hits": stats["rule_hits"],
        "warning_hits": stats["warning_hits"],
        "by_file": stats.get("by_file", {}),
        "load": stats["load"],
        "rules": describe_rules(get_rule_set(settings.validation_rules_file)),
    }

    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    return path


RECONCILE_SQL = """
SELECT
    (SELECT COUNT(*) FROM stg_healthcare_claims WHERE pipeline_run_id = :run_id) AS staged,
    (SELECT COUNT(*) FROM rejected_claims WHERE pipeline_run_id = :run_id) AS rejected,
    (SELECT COUNT(*) FROM fact_claims WHERE pipeline_run_id = :run_id) AS fact_rows
"""


def reconcile_run(
    stats: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    """
    Prove that every record is accounted for.

    In memory: received = valid + rejected, and received equals the rows read
    from the files. In the database: the run's staging and rejected rows
    match what was written, and fact rows stamped with the run fall between
    the rows it changed and the claims it merged. Raises ReconciliationError
    listing every check that failed.
    """

    load = stats.get("load")

    if load is None:
        return {"run_id": stats["run_id"], "checks": [], "passed": True}

    file_rows = sum(
        file["row_count"]
        for file in stats["files"]
        if file["status"] in (FILE_LOADED, FILE_ARCHIVED)
    )

    with get_engine().connect() as connection:
        db = connection.execute(text(RECONCILE_SQL), {"run_id": stats["run_id"]}).mappings().one()

    changed = load["inserted"] + load["updated"]

    checks = [
        ("received = valid + rejected", stats["records_received"], stats["records_valid"] + stats["records_rejected"]),
        ("received = rows read from files", stats["records_received"], file_rows),
        ("staged = valid", load["staged"], stats["records_valid"]),
        ("staging rows in database = staged", db["staged"], load["staged"]),
        ("rejected rows in database = rejected", db["rejected"], stats["records_rejected"]),
        ("merge candidates = inserted + updated + unchanged",
         load["merge_candidates"], changed + load["unchanged"]),
    ]

    results = [
        {"check": name, "actual": int(actual), "expected": int(expected), "passed": int(actual) == int(expected)}
        for name, actual, expected in checks
    ]

    # A retry of an already-committed run reports its rows as unchanged while
    # the fact rows still carry this run id, so this one is a range.
    fact_ok = changed <= db["fact_rows"] <= load["merge_candidates"]
    results.append(
        {
            "check": "inserted + updated <= fact rows for run <= merge candidates",
            "actual": int(db["fact_rows"]),
            "expected": f"{changed}..{load['merge_candidates']}",
            "passed": bool(fact_ok),
        }
    )

    failed = [result for result in results if not result["passed"]]

    if failed:
        raise ReconciliationError(
            "Reconciliation failed: "
            + "; ".join(f"{r['check']} (actual {r['actual']}, expected {r['expected']})" for r in failed)
        )

    logger.info("Run %s reconciled: %d checks passed", stats["run_id"], len(results))

    return {"run_id": stats["run_id"], "checks": results, "passed": True}


def alert_on_quality(
    stats: dict[str, Any],
    settings: Settings | None = None,
    manager: AlertManager | None = None,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()

    volumes = recent_volumes(stats["run_id"], settings.volume_lookback_runs) if stats.get("load") else []

    alerts = evaluate_quality_alerts(stats, settings, volumes)

    sent = (manager or AlertManager(settings)).dispatch(alerts)

    return [alert.to_dict() for alert in sent]


def _move(source: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    counter = 1
    while target.exists():
        target = target_dir / f"{source.stem}.{counter}{source.suffix}"
        counter += 1
    shutil.move(str(source), str(target))
    return target


def archive_run(
    stats: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    """
    Move the run's files out of the incoming directory.

    Only ever called after the load committed:
      loaded files   -> data/archive/<run_id>/
      skipped files  -> data/archive/<run_id>/skipped/   (already in the DB)
      rejected files -> data/rejected/<run_id>/          (quarantined for review)
    Leaving skipped and rejected files in place would make every scheduled run
    pick them up again.
    """

    settings = settings or get_settings()

    run_id = stats["run_id"]

    files = [SourceFile.from_dict(item) for item in stats["files"]]

    if not settings.archive_after_success:
        logger.info("archive_after_success is false; leaving files in place")
        return stats

    for file in files:

        source = Path(file.path)

        if not source.exists():
            # Already moved by an earlier attempt of this stage.
            continue

        if file.status == FILE_LOADED:
            target = _move(source, settings.archive_dir / run_id)
            file.status = FILE_ARCHIVED
        elif file.status == FILE_SKIPPED:
            target = _move(source, settings.archive_dir / run_id / "skipped")
        elif file.status == FILE_REJECTED:
            target = _move(source, settings.rejected_dir / run_id)
        else:
            continue

        file.archived_path = str(target.relative_to(settings.project_root))
        logger.info("Moved %s to %s", file.filename, file.archived_path)

    with transaction() as connection:
        record_source_files(connection, run_id, [f for f in files if f.archived_path])

    return _with_file_stats(dict(stats), files)


def finish_run(
    context: dict[str, Any],
    stats: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Write the final audit row and emit lineage COMPLETE."""

    settings = settings or get_settings()

    run_id = context["run_id"]

    load = stats.get("load") or {}

    if load:
        status = "PARTIAL" if stats.get("files_rejected") else "SUCCESS"
    else:
        status = "SKIPPED"

    record_audit(
        run_id=run_id,
        pipeline_name=settings.pipeline_name,
        source_filename=", ".join(file["filename"] for file in stats["files"])[:255],
        run_start_time=datetime.fromisoformat(context["started_at"]),
        run_end_time=utc_now(),
        records_received=stats["records_received"],
        records_loaded=load.get("staged", 0),
        records_rejected=stats["records_rejected"],
        records_flagged_duplicate=stats["records_flagged_duplicate"],
        records_inserted=load.get("inserted"),
        records_updated=load.get("updated"),
        records_unchanged=load.get("unchanged"),
        files_received=stats.get("files_received"),
        files_rejected=stats.get("files_rejected"),
        files_skipped=stats.get("files_skipped"),
        run_status=status,
    )

    emitter = LineageEmitter(settings)
    emitter.emit(
        emitter.build_event(
            "COMPLETE",
            run_id,
            [SourceFile.from_dict(f) for f in stats["files"] if f["status"] in (FILE_LOADED, FILE_ARCHIVED)],
            output_row_counts={
                "stg_healthcare_claims": load.get("staged", 0),
                "fact_claims": load.get("inserted", 0) + load.get("updated", 0),
                "rejected_claims": stats["records_rejected"],
            },
        )
    )

    logger.info("Run %s finished: %s", run_id, status)

    return {**stats, "run_status": status}


def fail_run(
    context: dict[str, Any] | None,
    error: BaseException | str,
    stage: str | None = None,
    settings: Settings | None = None,
    manager: AlertManager | None = None,
) -> None:
    """
    Record a failed run. Never raises: it runs while handling another error.

    Input files are deliberately left in the incoming directory so the next
    run (or a retry) picks them up again.
    """

    settings = settings or get_settings()

    run_id = (context or {}).get("run_id")

    logger.error("Run %s failed at stage %s: %s", run_id, stage, error)

    if context:
        try:
            started = context.get("started_at")
            record_audit(
                run_id=run_id,
                pipeline_name=settings.pipeline_name,
                source_filename=", ".join(f["filename"] for f in context.get("files", []))[:255],
                run_start_time=datetime.fromisoformat(started) if started else utc_now(),
                run_end_time=utc_now(),
                records_received=None,
                records_loaded=0,
                records_rejected=None,
                records_flagged_duplicate=None,
                run_status="FAILED",
                error_message=f"[{stage}] {error}"[:5000] if stage else str(error)[:5000],
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Could not write FAILED audit row for %s: %s", run_id, exc)

        try:
            emitter = LineageEmitter(settings)
            emitter.emit(emitter.build_event("FAIL", run_id, _files(context), error=str(error)))
        except Exception as exc:  # noqa: BLE001
            logger.error("Could not emit lineage FAIL for %s: %s", run_id, exc)

    (manager or AlertManager(settings)).dispatch([failure_alert(run_id, error, stage)])


# Single-process run

def run_pipeline(
    run_id: str | None = None,
    *,
    force_reprocess: bool = False,
    settings: Settings | None = None,
    alert_manager: AlertManager | None = None,
) -> dict[str, Any]:
    """
    Run every stage in this process. Returns the final stats dict.

    Returns {"run_status": "NO_FILES"} when there is nothing to do.
    """

    settings = settings or get_settings()

    configure_logging(settings)

    context: dict[str, Any] | None = None

    stage = "start"

    try:
        with run_lock(settings):

            try:
                context = start_run(run_id, force_reprocess=force_reprocess, settings=settings)
            except FileDiscoveryError as exc:
                logger.info("%s", exc)
                return {"run_status": "NO_FILES"}

            stage = "process"
            stats = process_run(context, settings)

            stage = "reconcile"
            stats["reconciliation"] = reconcile_run(stats, settings)

            stage = "quality_alerts"
            stats["alerts"] = alert_on_quality(stats, settings, alert_manager)

            stage = "archive"
            stats = archive_run(stats, settings)

            stage = "finish"
            return finish_run(context, stats, settings)

    except PipelineLockedError:
        raise

    except Exception as exc:
        fail_run(context, exc, stage, settings, alert_manager)
        raise


def print_run_summary(stats: dict[str, Any]) -> None:

    print()
    print("=" * 60)
    print(f"RUN {stats.get('run_id', '-')}: {stats.get('run_status')}")
    print("=" * 60)

    if stats.get("run_status") == "NO_FILES":
        print("No CSV files in the incoming directory.")
        print("=" * 60)
        return

    print(f"Files received / loaded / rejected / skipped: "
          f"{stats['files_received']} / {stats['files_loaded']} / "
          f"{stats['files_rejected']} / {stats['files_skipped']}")
    print(f"Records received:        {stats['records_received']:,}")
    print(f"Records valid:           {stats['records_valid']:,}")
    print(f"Records rejected:        {stats['records_rejected']:,}")
    print(f"Possible duplicates:     {stats['records_flagged_duplicate']:,}  (within batch)")

    load = stats.get("load")
    if load:
        print("-" * 60)
        print(f"Rows staged:             {load['staged']:,}")
        print(f"Merge candidates:        {load['merge_candidates']:,}")
        print(f"  inserted:              {load['inserted']:,}")
        print(f"  updated:               {load['updated']:,}")
        print(f"  unchanged:             {load['unchanged']:,}")
        print(f"Duplicate flags changed: {load['duplicate_flags_changed']:,}")

    reconciliation = stats.get("reconciliation") or {}
    if reconciliation.get("checks"):
        print("-" * 60)
        print(f"Reconciliation:          {len(reconciliation['checks'])} checks passed")

    for alert in stats.get("alerts") or []:
        print(f"ALERT [{alert['severity']}] {alert['code']}: {alert['message']}")

    print("=" * 60)


def main(argv: list[str] | None = None) -> int:

    parser = argparse.ArgumentParser(description="Run the healthcare claims pipeline.")
    parser.add_argument("--run-id", help="Reuse a run id, e.g. to retry a failed run.")
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Process files even if identical content was already loaded.",
    )
    parser.add_argument("--init-db", action="store_true", help="Apply schema, migrations and indexes first.")
    args = parser.parse_args(argv)

    settings = get_settings()

    configure_logging(settings)

    if args.init_db:
        from .database import init_database
        init_database()

    try:
        stats = run_pipeline(args.run_id, force_reprocess=args.force_reprocess, settings=settings)
    except PipelineLockedError as exc:
        logger.error("%s", exc)
        return 2
    except Exception:
        logger.exception("Pipeline run failed")
        return 1

    print_run_summary(stats)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
