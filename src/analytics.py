"""
Read-only analytics queries over the curated claims tables.

This is the single query layer behind both the REST API (src/api) and the
Streamlit dashboard (dashboard/app.py), so a metric means the same thing
wherever it is shown. sql/analytics_queries.sql remains the hand-run
reference version of the core reports.

Every function takes an open Connection and returns plain dicts. Filters are
always bound parameters; the only SQL fragments chosen at runtime come from
fixed whitelists (see _ORDER_BY), never from user input.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


@dataclass(frozen=True)
class ClaimFilters:
    service_date_from: date | None = None
    service_date_to: date | None = None
    claim_type: str | None = None
    insurance_plan: str | None = None
    claim_status: str | None = None
    provider_id: str | None = None

    def where(self, alias: str = "f") -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}

        if self.service_date_from:
            clauses.append(f"{alias}.service_date >= :service_date_from")
            params["service_date_from"] = self.service_date_from
        if self.service_date_to:
            clauses.append(f"{alias}.service_date <= :service_date_to")
            params["service_date_to"] = self.service_date_to
        for column in ("claim_type", "insurance_plan", "claim_status", "provider_id"):
            value = getattr(self, column)
            if value:
                clauses.append(f"{alias}.{column} = :{column}")
                params[column] = value

        return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def _rows(connection: Connection, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(text(sql), params).mappings()]


# Claims

def kpis(connection: Connection, filters: ClaimFilters = ClaimFilters()) -> dict[str, Any]:
    where, params = filters.where()
    sql = f"""
        SELECT
            COUNT(*)                                        AS total_claims,
            COALESCE(SUM(f.billed_amount), 0)               AS total_billed,
            COALESCE(SUM(f.approved_amount), 0)             AS total_approved,
            COALESCE(SUM(f.paid_amount), 0)                 AS total_paid,
            COALESCE(SUM(f.unpaid_amount), 0)               AS total_unpaid,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_approved) / NULLIF(COUNT(*), 0), 2)
                                                            AS approval_rate_percent,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_denied) / NULLIF(COUNT(*), 0), 2)
                                                            AS denial_rate_percent,
            ROUND(AVG(f.processing_days), 2)                AS avg_processing_days,
            COUNT(*) FILTER (WHERE f.duplicate_flag)        AS possible_duplicates,
            COUNT(*) FILTER (WHERE f.high_value_flag)       AS high_value_claims,
            COUNT(*) FILTER (WHERE f.late_submission_flag)  AS late_submissions,
            MAX(f.updated_at)                               AS data_as_of
        FROM fact_claims f
        {where}
    """
    return _rows(connection, sql, params)[0]


def claims_by_status(connection: Connection, filters: ClaimFilters = ClaimFilters()) -> list[dict[str, Any]]:
    where, params = filters.where()
    sql = f"""
        SELECT
            f.claim_status,
            COUNT(*)                        AS claim_count,
            COALESCE(SUM(f.billed_amount), 0)   AS total_billed,
            COALESCE(SUM(f.approved_amount), 0) AS total_approved,
            COALESCE(SUM(f.paid_amount), 0)     AS total_paid,
            COALESCE(SUM(f.unpaid_amount), 0)   AS total_unpaid
        FROM fact_claims f
        {where}
        GROUP BY f.claim_status
        ORDER BY claim_count DESC
    """
    return _rows(connection, sql, params)


def monthly_trends(connection: Connection, filters: ClaimFilters = ClaimFilters()) -> list[dict[str, Any]]:
    where, params = filters.where()
    sql = f"""
        SELECT
            DATE_TRUNC('month', f.service_date)::date AS service_month,
            COUNT(*)                            AS claim_count,
            COALESCE(SUM(f.billed_amount), 0)   AS total_billed,
            COALESCE(SUM(f.approved_amount), 0) AS total_approved,
            COALESCE(SUM(f.paid_amount), 0)     AS total_paid,
            COUNT(*) FILTER (WHERE f.is_denied) AS denied_count
        FROM fact_claims f
        {where}
        GROUP BY service_month
        ORDER BY service_month
    """
    return _rows(connection, sql, params)


def top_denial_reasons(
    connection: Connection,
    filters: ClaimFilters = ClaimFilters(),
    limit: int = 10,
) -> list[dict[str, Any]]:
    where, params = filters.where()
    where = f"{where} AND f.is_denied" if where else "WHERE f.is_denied"
    sql = f"""
        SELECT
            COALESCE(f.denial_reason, '(none)') AS denial_reason,
            COUNT(*)                            AS denial_count,
            COALESCE(SUM(f.billed_amount), 0)   AS billed_amount,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_percent
        FROM fact_claims f
        {where}
        GROUP BY 1
        ORDER BY denial_count DESC
        LIMIT :limit
    """
    return _rows(connection, sql, {**params, "limit": limit})


_ORDER_BY = {
    "billed": "total_billed DESC, total_paid DESC",
    "paid": "total_paid DESC, total_billed DESC",
    "claims": "claim_count DESC, total_billed DESC",
}


def top_providers(
    connection: Connection,
    filters: ClaimFilters = ClaimFilters(),
    limit: int = 10,
    order_by: str = "billed",
) -> list[dict[str, Any]]:
    if order_by not in _ORDER_BY:
        raise ValueError(f"order_by must be one of {sorted(_ORDER_BY)}")
    where, params = filters.where()
    sql = f"""
        SELECT
            f.provider_id,
            COUNT(*)                            AS claim_count,
            COALESCE(SUM(f.billed_amount), 0)   AS total_billed,
            COALESCE(SUM(f.paid_amount), 0)     AS total_paid,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_denied) / NULLIF(COUNT(*), 0), 2)
                                                AS denial_rate_percent
        FROM fact_claims f
        {where}
        GROUP BY f.provider_id
        ORDER BY {_ORDER_BY[order_by]}
        LIMIT :limit
    """
    return _rows(connection, sql, {**params, "limit": limit})


def processing_time_by_type(connection: Connection, filters: ClaimFilters = ClaimFilters()) -> list[dict[str, Any]]:
    where, params = filters.where()
    sql = f"""
        SELECT
            COALESCE(f.claim_type, '(unknown)') AS claim_type,
            COUNT(*)                            AS claim_count,
            ROUND(AVG(f.processing_days), 2)    AS avg_processing_days,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.processing_days) AS median_processing_days,
            MAX(f.processing_days)              AS max_processing_days
        FROM fact_claims f
        {where}
        GROUP BY 1
        ORDER BY avg_processing_days DESC
    """
    return _rows(connection, sql, params)


def high_value_claims(
    connection: Connection,
    filters: ClaimFilters = ClaimFilters(),
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where, params = filters.where()
    where = f"{where} AND f.high_value_flag" if where else "WHERE f.high_value_flag"
    sql = f"""
        SELECT
            f.claim_id, f.provider_id, f.service_date, f.claim_type, f.claim_status,
            f.billed_amount, f.approved_amount, f.paid_amount, f.duplicate_flag,
            COUNT(*) OVER () AS total_count
        FROM fact_claims f
        {where}
        ORDER BY f.billed_amount DESC, f.claim_id
        LIMIT :limit OFFSET :offset
    """
    rows = _rows(connection, sql, {**params, "limit": limit, "offset": offset})
    return _page(rows, limit, offset)


def duplicate_groups(
    connection: Connection,
    filters: ClaimFilters = ClaimFilters(),
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where, params = filters.where()
    where = f"{where} AND f.duplicate_flag" if where else "WHERE f.duplicate_flag"
    sql = f"""
        SELECT
            f.patient_id, f.provider_id, f.service_date, f.procedure_code, f.billed_amount,
            COUNT(*)                              AS claim_count,
            ARRAY_AGG(f.claim_id ORDER BY f.claim_id) AS claim_ids,
            COUNT(*) OVER ()                      AS total_count
        FROM fact_claims f
        {where}
        GROUP BY f.patient_id, f.provider_id, f.service_date, f.procedure_code, f.billed_amount
        ORDER BY claim_count DESC, f.service_date DESC
        LIMIT :limit OFFSET :offset
    """
    rows = _rows(connection, sql, {**params, "limit": limit, "offset": offset})
    return _page(rows, limit, offset)


def _page(rows: list[dict[str, Any]], limit: int, offset: int) -> dict[str, Any]:
    total = int(rows[0]["total_count"]) if rows else 0
    for row in rows:
        row.pop("total_count", None)
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


def claim_detail(connection: Connection, claim_id: str) -> dict[str, Any] | None:
    rows = _rows(
        connection,
        "SELECT * FROM fact_claims WHERE claim_id = :claim_id",
        {"claim_id": claim_id},
    )
    return rows[0] if rows else None


def filter_options(connection: Connection) -> dict[str, Any]:
    """Distinct values for dashboard and client filter pickers."""

    row = _rows(
        connection,
        """
        SELECT
            ARRAY(SELECT DISTINCT claim_type FROM fact_claims WHERE claim_type IS NOT NULL ORDER BY 1)
                AS claim_types,
            ARRAY(SELECT DISTINCT insurance_plan FROM fact_claims WHERE insurance_plan IS NOT NULL ORDER BY 1)
                AS insurance_plans,
            ARRAY(SELECT DISTINCT claim_status FROM fact_claims ORDER BY 1) AS claim_statuses,
            (SELECT MIN(service_date) FROM fact_claims) AS min_service_date,
            (SELECT MAX(service_date) FROM fact_claims) AS max_service_date
        """,
        {},
    )[0]
    return row


# Pipeline operations and data quality

def pipeline_runs(connection: Connection, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
    where = "WHERE run_status = :status" if status else ""
    sql = f"""
        SELECT
            pipeline_run_id, pipeline_name, run_status, run_start_time, run_end_time,
            EXTRACT(EPOCH FROM (run_end_time - run_start_time))::NUMERIC(12,2) AS duration_seconds,
            files_received, files_rejected, files_skipped,
            records_received, records_loaded, records_rejected, records_flagged_duplicate,
            records_inserted, records_updated, records_unchanged,
            error_message
        FROM pipeline_audit
        {where}
        ORDER BY run_start_time DESC NULLS LAST
        LIMIT :limit
    """
    return _rows(connection, sql, {"limit": limit, "status": status})


def pipeline_run(connection: Connection, run_id: str) -> dict[str, Any] | None:
    runs = _rows(
        connection,
        """
        SELECT *, EXTRACT(EPOCH FROM (run_end_time - run_start_time))::NUMERIC(12,2) AS duration_seconds
        FROM pipeline_audit WHERE pipeline_run_id = :run_id
        """,
        {"run_id": run_id},
    )
    if not runs:
        return None
    run = runs[0]
    run["files"] = _rows(
        connection,
        """
        SELECT filename, sha256, size_bytes, row_count, status, error_message, archived_path, recorded_at
        FROM source_files WHERE pipeline_run_id = :run_id ORDER BY filename
        """,
        {"run_id": run_id},
    )
    run["rejection_reasons"] = rejection_reasons(connection, run_id=run_id)
    run["alerts"] = recent_alerts(connection, run_id=run_id)
    return run


def rejection_reasons(connection: Connection, run_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """
    Count rejection codes. A record with "A|B" counts once under A and once
    under B, so the counts can sum to more than the number of records.
    """

    where = "WHERE r.pipeline_run_id = :run_id" if run_id else ""
    sql = f"""
        SELECT
            code AS rejection_code,
            COUNT(*) AS record_count
        FROM rejected_claims r
        CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(r.rejection_reason, '|')) AS code
        {where}
        GROUP BY code
        ORDER BY record_count DESC
        LIMIT :limit
    """
    return _rows(connection, sql, {"run_id": run_id, "limit": limit})


def rejected_records(
    connection: Connection,
    run_id: str | None = None,
    rejection_code: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    clauses, params = [], {"limit": limit, "offset": offset}
    if run_id:
        clauses.append("r.pipeline_run_id = :run_id")
        params["run_id"] = run_id
    if rejection_code:
        clauses.append(":code = ANY(STRING_TO_ARRAY(r.rejection_reason, '|'))")
        params["code"] = rejection_code
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT r.rejection_id, r.claim_id, r.rejection_reason, r.source_filename,
               r.source_row_number, r.pipeline_run_id, r.rejected_at, r.raw_record,
               COUNT(*) OVER () AS total_count
        FROM rejected_claims r
        {where}
        ORDER BY r.rejected_at DESC, r.rejection_id DESC
        LIMIT :limit OFFSET :offset
    """
    return _page(_rows(connection, sql, params), limit, offset)


def quality_trend(connection: Connection, limit: int = 30) -> list[dict[str, Any]]:
    """Rejection and duplicate rates per completed run, oldest first."""

    sql = """
        SELECT * FROM (
            SELECT
                pipeline_run_id, run_start_time, run_status, records_received,
                ROUND(100.0 * records_rejected / NULLIF(records_received, 0), 2)
                    AS rejection_rate_percent,
                ROUND(100.0 * records_flagged_duplicate / NULLIF(records_received, 0), 2)
                    AS duplicate_rate_percent
            FROM pipeline_audit
            WHERE run_status IN ('SUCCESS', 'PARTIAL') AND records_received > 0
            ORDER BY run_start_time DESC
            LIMIT :limit
        ) recent
        ORDER BY run_start_time
    """
    return _rows(connection, sql, {"limit": limit})


def recent_alerts(connection: Connection, limit: int = 50, run_id: str | None = None) -> list[dict[str, Any]]:
    where = "WHERE pipeline_run_id = :run_id" if run_id else ""
    sql = f"""
        SELECT alert_id, pipeline_run_id, alert_code, severity, title, message,
               details, channels_delivered, created_at
        FROM pipeline_alerts
        {where}
        ORDER BY created_at DESC, alert_id DESC
        LIMIT :limit
    """
    return _rows(connection, sql, {"limit": limit, "run_id": run_id})
