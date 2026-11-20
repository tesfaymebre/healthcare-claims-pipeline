"""
Data lineage for the healthcare claims pipeline.

Lineage is recorded at three levels:

Row    Every staged, rejected and fact row carries source_filename,
       source_row_number and pipeline_run_id (added during ingestion).
       fact_claims_history keeps every version of a claim.
File   source_files: one row per input file per run, with its SHA-256,
       row count, status and archive location. The checksum also drives
       file-level duplicate control.
Run    pipeline_audit, plus OpenLineage RunEvents (START / COMPLETE / FAIL)
       describing which files fed which tables. OpenLineage is the open
       standard that Marquez, DataHub and Airflow's lineage integration
       consume, so the events can be shipped to any of them unchanged.

trace_claim() stitches the levels together to follow one claim from its
source record to the fact table.
"""

from __future__ import annotations

import json
import logging
import socket
import urllib.request
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .config import Settings, get_settings
from .ingestion import FILE_ACCEPTED, FILE_ARCHIVED, FILE_LOADED, SourceFile


logger = logging.getLogger("healthcare_claims_pipeline.lineage")

OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"

PRODUCER = "urn:healthcare-claims-pipeline"


# File-level lineage

PREVIOUSLY_LOADED_SQL = """
SELECT DISTINCT ON (sha256)
    sha256,
    filename,
    pipeline_run_id
FROM source_files
WHERE sha256 = ANY(:checksums)
  AND status IN ('LOADED', 'ARCHIVED')
ORDER BY sha256, recorded_at
"""

UPSERT_SOURCE_FILE_SQL = """
INSERT INTO source_files (
    pipeline_run_id,
    filename,
    sha256,
    size_bytes,
    row_count,
    status,
    error_message,
    archived_path
) VALUES (
    :pipeline_run_id,
    :filename,
    :sha256,
    :size_bytes,
    :row_count,
    :status,
    :error_message,
    :archived_path
)
ON CONFLICT (pipeline_run_id, sha256, filename) DO UPDATE
SET
    size_bytes = EXCLUDED.size_bytes,
    row_count = EXCLUDED.row_count,
    status = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    archived_path = COALESCE(EXCLUDED.archived_path, source_files.archived_path),
    recorded_at = CURRENT_TIMESTAMP
"""


def previously_loaded(
    connection: Connection,
    checksums: Iterable[str],
) -> dict[str, dict[str, str]]:
    """
    Map sha256 -> {filename, pipeline_run_id} of the first successful load.
    """

    checksums = list(checksums)

    if not checksums:
        return {}

    rows = connection.execute(
        text(PREVIOUSLY_LOADED_SQL),
        {"checksums": checksums},
    ).mappings()

    return {
        row["sha256"]: {
            "filename": row["filename"],
            "pipeline_run_id": row["pipeline_run_id"],
        }
        for row in rows
    }


def record_source_files(
    connection: Connection,
    run_id: str,
    files: Iterable[SourceFile],
    *,
    committed: bool = False,
) -> None:
    """
    Upsert this run's file rows.

    committed=True is passed from inside the load transaction: accepted files
    are recorded as LOADED in the same commit as their rows, so a file is
    never marked loaded unless its data really landed.
    """

    records = []

    for file in files:

        status = FILE_LOADED if committed and file.status == FILE_ACCEPTED else file.status

        records.append(
            {
                "pipeline_run_id": run_id,
                "filename": file.filename,
                "sha256": file.sha256,
                "size_bytes": file.size_bytes,
                "row_count": file.row_count,
                "status": status,
                "error_message": file.error,
                "archived_path": file.archived_path,
            }
        )

    if records:
        connection.execute(text(UPSERT_SOURCE_FILE_SQL), records)


def mark_files_archived(
    connection: Connection,
    run_id: str,
    files: Iterable[SourceFile],
) -> None:
    archived = [file for file in files if file.status == FILE_ARCHIVED]
    record_source_files(connection, run_id, archived)


# Claim-level lineage

TRACE_QUERIES: dict[str, str] = {
    "fact": """
        SELECT *
        FROM fact_claims
        WHERE claim_id = :claim_id
    """,
    "history": """
        SELECT history_id, change_type, claim_status, approved_amount,
               paid_amount, denial_reason, source_filename,
               source_row_number, pipeline_run_id, recorded_at
        FROM fact_claims_history
        WHERE claim_id = :claim_id
        ORDER BY recorded_at, history_id
    """,
    "staging": """
        SELECT claim_status, approved_amount, paid_amount, source_filename,
               source_row_number, pipeline_run_id, loaded_at
        FROM stg_healthcare_claims
        WHERE claim_id = :claim_id
        ORDER BY loaded_at
    """,
    "rejections": """
        SELECT rejection_id, rejection_reason, raw_record, source_filename,
               source_row_number, pipeline_run_id, rejected_at
        FROM rejected_claims
        WHERE claim_id = :claim_id
        ORDER BY rejected_at
    """,
}

TRACE_RUNS_SQL = """
SELECT pipeline_run_id, run_status, run_start_time, run_end_time,
       records_received, records_loaded, records_rejected
FROM pipeline_audit
WHERE pipeline_run_id = ANY(:run_ids)
ORDER BY run_start_time
"""

TRACE_FILES_SQL = """
SELECT pipeline_run_id, filename, sha256, row_count, status, archived_path
FROM source_files
WHERE pipeline_run_id = ANY(:run_ids)
  AND filename = ANY(:filenames)
ORDER BY recorded_at
"""


def trace_claim(connection: Connection, claim_id: str) -> dict[str, Any]:
    """
    Everything the pipeline knows about one claim, from source file to fact.

    Returns empty lists (and fact=None) when the claim was never seen.
    """

    result: dict[str, Any] = {"claim_id": claim_id}

    for key, sql in TRACE_QUERIES.items():
        rows = [dict(row) for row in connection.execute(text(sql), {"claim_id": claim_id}).mappings()]
        result[key] = rows

    result["fact"] = result["fact"][0] if result["fact"] else None

    run_ids = {
        row["pipeline_run_id"]
        for key in ("history", "staging", "rejections")
        for row in result[key]
        if row.get("pipeline_run_id")
    }
    if result["fact"] and result["fact"].get("pipeline_run_id"):
        run_ids.add(result["fact"]["pipeline_run_id"])

    filenames = {
        row["source_filename"]
        for key in ("history", "staging", "rejections")
        for row in result[key]
        if row.get("source_filename")
    }
    if result["fact"] and result["fact"].get("source_filename"):
        filenames.add(result["fact"]["source_filename"])

    params = {"run_ids": sorted(run_ids), "filenames": sorted(filenames)}

    result["runs"] = [dict(row) for row in connection.execute(text(TRACE_RUNS_SQL), params).mappings()]

    result["source_files"] = [
        dict(row) for row in connection.execute(text(TRACE_FILES_SQL), params).mappings()
    ]

    return result


# Run-level lineage: OpenLineage events

def _openlineage_run_id(run_id: str) -> str:
    """OpenLineage requires a UUID; map any run id to one, deterministically."""

    try:
        return str(uuid.UUID(hex=run_id))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"healthcare-claims/{run_id}"))


def _facet(**fields: Any) -> dict[str, Any]:
    return {"_producer": PRODUCER, "_schemaURL": OPENLINEAGE_SCHEMA_URL, **fields}


class LineageEmitter:
    """
    Builds and ships OpenLineage RunEvents.

    Emission is best effort: a lineage backend being down must never fail a
    pipeline run, so errors are logged and the event falls back to the file
    transport.
    """

    TABLE_OUTPUTS = ("stg_healthcare_claims", "fact_claims", "rejected_claims")

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    @property
    def enabled(self) -> bool:
        return self.settings.lineage_enabled and self.settings.lineage_transport != "none"

    def _dataset_namespace(self) -> str:
        db = self.settings.database
        return f"postgres://{db.host}:{db.port}"

    def _table_name(self, table: str) -> str:
        return f"{self.settings.database.database}.public.{table}"

    def build_event(
        self,
        event_type: str,
        run_id: str,
        files: Iterable[SourceFile] = (),
        output_row_counts: dict[str, int] | None = None,
        error: str | None = None,
        extra_run_facets: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        run_facets: dict[str, Any] = {
            "pipelineRun": _facet(
                pipelineRunId=run_id,
                pipelineVersion=self.settings.pipeline_version,
            ),
        }

        if error:
            run_facets["errorMessage"] = _facet(
                message=error[:2000],
                programmingLanguage="python",
            )

        if extra_run_facets:
            run_facets.update(extra_run_facets)

        source_fields = [{"name": column, "type": "string"} for column in self.settings.source_columns]

        inputs = [
            {
                "namespace": f"file://{socket.gethostname()}",
                "name": file.path,
                "facets": {
                    "schema": _facet(fields=source_fields),
                },
                "inputFacets": {
                    "dataQualityMetrics": _facet(rowCount=file.row_count),
                },
            }
            for file in files
        ]

        outputs = []

        if output_row_counts is not None:
            for table in self.TABLE_OUTPUTS:
                outputs.append(
                    {
                        "namespace": self._dataset_namespace(),
                        "name": self._table_name(table),
                        "outputFacets": {
                            "outputStatistics": _facet(rowCount=int(output_row_counts.get(table, 0))),
                        },
                    }
                )

        return {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "producer": PRODUCER,
            "schemaURL": OPENLINEAGE_SCHEMA_URL,
            "run": {"runId": _openlineage_run_id(run_id), "facets": run_facets},
            "job": {
                "namespace": self.settings.lineage_namespace,
                "name": self.settings.pipeline_name,
                "facets": {},
            },
            "inputs": inputs,
            "outputs": outputs,
        }

    def emit(self, event: dict[str, Any]) -> None:

        if not self.enabled:
            return

        if self.settings.lineage_transport == "http":
            if self._emit_http(event):
                return

        self._emit_file(event)

    def _emit_http(self, event: dict[str, Any]) -> bool:

        url = self.settings.openlineage_url

        if not url:
            logger.warning("lineage.transport is http but OPENLINEAGE_URL is not set; writing to file")
            return False

        request = urllib.request.Request(
            url.rstrip("/") + "/api/v1/lineage",
            data=json.dumps(event, default=str).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=5):
                return True
        except Exception as exc:  # noqa: BLE001 - lineage must never fail the run
            logger.warning("OpenLineage HTTP emit failed (%s); writing to file", exc)
            return False

    def _emit_file(self, event: dict[str, Any]) -> None:

        try:
            directory = self.settings.lineage_dir
            directory.mkdir(parents=True, exist_ok=True)
            day = event["eventTime"][:10]
            with (directory / f"openlineage_{day}.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, default=str) + "\n")
        except Exception as exc:  # noqa: BLE001
            logger.warning("OpenLineage file emit failed: %s", exc)
