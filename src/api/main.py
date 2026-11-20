"""
REST API for claims summaries, pipeline operations and lineage.

Run:
    uvicorn src.api.main:app --reload

Interactive docs at http://localhost:8000/docs.

Design notes
- Read-only: every request uses a read-only database session.
- Versioned under /api/v1 so the contract can evolve without breaking clients.
- Summary endpoints return aggregates. Record-level endpoints (a claim, its
  lineage, rejected records, duplicate groups) expose identifiers, so set
  API_KEY anywhere but a developer machine.
- Database outages surface as 503 with a generic message; details go to the
  server log, not to the client.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .. import analytics
from ..config import get_settings
from ..database import DatabaseError, get_engine
from ..lineage import trace_claim
from ..rules import describe_rules, get_rule_set
from . import schemas
from .dependencies import ConnectionDep, FiltersDep, PagingDep, SettingsDep, require_api_key


logger = logging.getLogger("healthcare_claims_pipeline.api")

ClaimId = Annotated[str, Path(min_length=1, max_length=50)]
RunId = Annotated[str, Path(min_length=1, max_length=50)]
TopN = Annotated[int, Query(ge=1, le=100)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.api_key is None:
        logger.warning("API_KEY is not set: the API is unauthenticated. Set it outside local development.")
    yield
    get_engine().dispose()


app = FastAPI(
    title="Healthcare Claims API",
    version=get_settings().pipeline_version,
    description="Read-only access to curated claims metrics, pipeline runs, data quality and lineage.",
    lifespan=lifespan,
)


@app.exception_handler(SQLAlchemyError)
@app.exception_handler(DatabaseError)
async def database_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Database error on %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Database unavailable"},
    )


@app.get("/health", response_model=schemas.Health, tags=["health"])
def health(settings: SettingsDep) -> schemas.Health | JSONResponse:
    """Liveness plus database reachability. Unauthenticated, for load balancers."""

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))
        database = "ok"
    except SQLAlchemyError:
        database = "unreachable"

    body = schemas.Health(
        status="ok" if database == "ok" else "degraded",
        database=database,
        version=settings.pipeline_version,
    )

    if database != "ok":
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=body.model_dump())

    return body


v1 = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


# Claims summaries

@v1.get("/claims/summary", response_model=schemas.ClaimsSummary, tags=["claims"])
def claims_summary(connection: ConnectionDep, filters: FiltersDep):
    """Headline KPIs: counts, money totals, approval/denial rates, flags."""
    return analytics.kpis(connection, filters)


@v1.get("/claims/by-status", response_model=list[schemas.StatusBreakdown], tags=["claims"])
def claims_by_status(connection: ConnectionDep, filters: FiltersDep):
    return analytics.claims_by_status(connection, filters)


@v1.get("/claims/monthly", response_model=list[schemas.MonthlyTrend], tags=["claims"])
def claims_monthly(connection: ConnectionDep, filters: FiltersDep):
    """Claim volume and payment totals per service month."""
    return analytics.monthly_trends(connection, filters)


@v1.get("/claims/processing-time", response_model=list[schemas.ProcessingTime], tags=["claims"])
def claims_processing_time(connection: ConnectionDep, filters: FiltersDep):
    return analytics.processing_time_by_type(connection, filters)


@v1.get("/claims/high-value", response_model=schemas.Page[schemas.HighValueClaim], tags=["claims"])
def claims_high_value(connection: ConnectionDep, filters: FiltersDep, page: PagingDep):
    """Claims above validation.high_value_threshold, largest first."""
    return analytics.high_value_claims(connection, filters, page.limit, page.offset)


@v1.get("/claims/duplicates", response_model=schemas.Page[schemas.DuplicateGroup], tags=["claims"])
def claims_duplicates(connection: ConnectionDep, filters: FiltersDep, page: PagingDep):
    """Groups of claims sharing the duplicate business key."""
    return analytics.duplicate_groups(connection, filters, page.limit, page.offset)


@v1.get("/claims/filters", response_model=schemas.FilterOptions, tags=["claims"])
def claims_filter_options(connection: ConnectionDep):
    """Valid values for the filter query parameters."""
    return analytics.filter_options(connection)


@v1.get("/claims/{claim_id}", response_model=schemas.Claim, tags=["claims"])
def claim(claim_id: ClaimId, connection: ConnectionDep):
    row = analytics.claim_detail(connection, claim_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
    return row


@v1.get("/denials/top", response_model=list[schemas.DenialReason], tags=["claims"])
def denials_top(connection: ConnectionDep, filters: FiltersDep, limit: TopN = 10):
    return analytics.top_denial_reasons(connection, filters, limit)


@v1.get("/providers/top", response_model=list[schemas.ProviderTotals], tags=["claims"])
def providers_top(
    connection: ConnectionDep,
    filters: FiltersDep,
    limit: TopN = 10,
    order_by: Literal["billed", "paid", "claims"] = "billed",
):
    return analytics.top_providers(connection, filters, limit, order_by)


# Pipeline operations

@v1.get("/pipeline/runs", response_model=list[schemas.PipelineRun], tags=["pipeline"])
def pipeline_runs(
    connection: ConnectionDep,
    limit: TopN = 20,
    run_status: Annotated[
        Literal["RUNNING", "SUCCESS", "FAILED", "PARTIAL", "SKIPPED"] | None,
        Query(alias="status"),
    ] = None,
):
    return analytics.pipeline_runs(connection, limit, run_status)


@v1.get("/pipeline/runs/{run_id}", response_model=schemas.PipelineRunDetail, tags=["pipeline"])
def pipeline_run(run_id: RunId, connection: ConnectionDep):
    """One run with its files, rejection breakdown and alerts."""
    run = analytics.pipeline_run(connection, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return run


@v1.get("/alerts", response_model=list[schemas.AlertRecord], tags=["pipeline"])
def alerts(connection: ConnectionDep, limit: TopN = 50, run_id: str | None = None):
    return analytics.recent_alerts(connection, limit, run_id)


# Data quality

@v1.get("/quality/rejections", response_model=list[schemas.RejectionCount], tags=["quality"])
def quality_rejections(connection: ConnectionDep, run_id: str | None = None):
    """Rejection-code counts; a record failing two rules counts under both."""
    return analytics.rejection_reasons(connection, run_id)


@v1.get("/quality/rejected-records", response_model=schemas.Page[schemas.RejectedRecord], tags=["quality"])
def quality_rejected_records(
    connection: ConnectionDep,
    page: PagingDep,
    run_id: str | None = None,
    rejection_code: Annotated[str | None, Query(pattern=r"^[A-Z][A-Z0-9_]*$")] = None,
):
    return analytics.rejected_records(connection, run_id, rejection_code, page.limit, page.offset)


@v1.get("/quality/rules", response_model=list[schemas.ValidationRuleInfo], tags=["quality"])
def quality_rules(settings: SettingsDep):
    """The validation rules currently configured in config/validation_rules.yaml."""
    return describe_rules(get_rule_set(settings.validation_rules_file))


# Lineage

@v1.get("/lineage/claims/{claim_id}", response_model=schemas.ClaimLineage, tags=["lineage"])
def lineage_claim(claim_id: ClaimId, connection: ConnectionDep):
    """
    Trace a claim through every stage: source file and row, staging,
    rejections, each version written to the fact table, and the runs involved.
    """
    trace = trace_claim(connection, claim_id)
    if trace["fact"] is None and not trace["rejections"] and not trace["staging"]:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} was never seen by the pipeline")
    return trace


app.include_router(v1)
