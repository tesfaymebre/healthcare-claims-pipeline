"""
Airflow orchestration for the healthcare claims pipeline (Airflow 3.x).

The DAG is deliberately thin: every task calls a stage function from
src/pipeline.py, the same functions `python -m src.pipeline` runs in one
process. Business logic lives in the package, where it is unit tested;
the DAG only decides order, retries, timeouts and failure handling.

    wait_for_files -> start_run -> process -> reconcile -> quality_alerts
                                                        \\-> archive -> finish

Design choices
- max_active_runs=1: two overlapping runs would read the same files.
- The pipeline run id is derived from the Airflow run id, so a retried or
  cleared task reuses it and the loader replaces the earlier attempt
  instead of stacking a second copy (idempotent retries).
- Only `process` touches the claims tables, in a single transaction; it is
  the task that gets retries with exponential backoff.
- Data never travels through XCom, only small dicts of ids and counts.
- The sensor runs in reschedule mode (it frees its worker slot between
  pokes) and soft-fails, so an hour with no files ends as "skipped",
  not as a failure that pages someone.
- The failure callback fires only once retries are exhausted. It marks the
  audit row FAILED, emits an OpenLineage FAIL event and raises a CRITICAL
  alert. Input files stay in data/incoming for the next run.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.bases.sensor import PokeReturnValue

DAG_ID = "healthcare_claims_pipeline"


def _on_task_failure(context) -> None:
    # Imported lazily: DAG files are parsed often and must stay cheap to load.
    from src.pipeline import fail_run

    ti = context["ti"]
    run_context = ti.xcom_pull(task_ids="start_run")
    fail_run(run_context, context.get("exception") or "Task failed", stage=ti.task_id)


default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_task_failure,
}


@dag(
    dag_id=DAG_ID,
    description="Ingest, validate, load and audit synthetic healthcare claims.",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "force_reprocess": Param(
            False,
            type="boolean",
            description="Process files even if identical content was already loaded.",
        ),
    },
    tags=["claims", "etl", "postgres"],
    doc_md=__doc__,
)
def healthcare_claims_pipeline():

    @task.sensor(
        poke_interval=60,
        timeout=55 * 60,
        mode="reschedule",
        soft_fail=True,
        on_failure_callback=None,
    )
    def wait_for_files() -> PokeReturnValue:
        from src.config import get_settings

        files = sorted(path.name for path in get_settings().incoming_dir.glob("*.csv"))
        return PokeReturnValue(is_done=bool(files), xcom_value=files)

    @task
    def start_run(_files: list[str]) -> dict:
        from src.pipeline import deterministic_run_id
        from src.pipeline import start_run as start

        context = get_current_context()
        run_id = deterministic_run_id(DAG_ID, context["run_id"])
        return start(run_id, force_reprocess=bool(context["params"]["force_reprocess"]))

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
    )
    def process(run_context: dict) -> dict:
        from src.pipeline import process_run

        return process_run(run_context)

    @task
    def reconcile(stats: dict) -> dict:
        from src.pipeline import reconcile_run

        return {**stats, "reconciliation": reconcile_run(stats)}

    @task
    def quality_alerts(stats: dict) -> list[dict]:
        from src.pipeline import alert_on_quality

        return alert_on_quality(stats)

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def archive(stats: dict) -> dict:
        from src.pipeline import archive_run

        return archive_run(stats)

    @task
    def finish(run_context: dict, stats: dict) -> dict:
        from src.pipeline import finish_run

        result = finish_run(run_context, stats)
        return {key: result[key] for key in ("run_id", "run_status", "records_received", "records_rejected")}

    files = wait_for_files()
    run_context = start_run(files)
    stats = process(run_context)
    reconciled = reconcile(stats)
    alerts = quality_alerts(reconciled)
    archived = archive(reconciled)
    alerts >> archived
    finish(run_context, archived)


healthcare_claims_pipeline()
