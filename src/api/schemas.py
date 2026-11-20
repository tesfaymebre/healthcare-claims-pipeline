"""
Response models for the claims API.

Explicit models (rather than returning raw rows) give the API a stable,
documented contract in /docs, and stop a new column in a table from leaking
into responses unannounced.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, PlainSerializer

# Amounts are exact Decimals internally and plain JSON numbers on the wire.
Money = Annotated[Decimal, PlainSerializer(lambda value: float(value), return_type=float, when_used="json")]
Rate = Annotated[Decimal | None, PlainSerializer(lambda v: None if v is None else float(v), when_used="json")]

T = TypeVar("T")


class _Model(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class Page(_Model, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int


class Health(_Model):
    status: str
    database: str
    version: str


class ClaimsSummary(_Model):
    total_claims: int
    total_billed: Money
    total_approved: Money
    total_paid: Money
    total_unpaid: Money
    approval_rate_percent: Rate
    denial_rate_percent: Rate
    avg_processing_days: Rate
    possible_duplicates: int
    high_value_claims: int
    late_submissions: int
    data_as_of: datetime | None


class StatusBreakdown(_Model):
    claim_status: str
    claim_count: int
    total_billed: Money
    total_approved: Money
    total_paid: Money
    total_unpaid: Money


class MonthlyTrend(_Model):
    service_month: date
    claim_count: int
    total_billed: Money
    total_approved: Money
    total_paid: Money
    denied_count: int


class DenialReason(_Model):
    denial_reason: str
    denial_count: int
    billed_amount: Money
    share_percent: Rate


class ProviderTotals(_Model):
    provider_id: str
    claim_count: int
    total_billed: Money
    total_paid: Money
    denial_rate_percent: Rate


class ProcessingTime(_Model):
    claim_type: str
    claim_count: int
    avg_processing_days: Rate
    median_processing_days: float | None
    max_processing_days: int | None


class HighValueClaim(_Model):
    claim_id: str
    provider_id: str
    service_date: date
    claim_type: str | None
    claim_status: str
    billed_amount: Money
    approved_amount: Money | None
    paid_amount: Money | None
    duplicate_flag: bool | None


class DuplicateGroup(_Model):
    patient_id: str
    provider_id: str
    service_date: date
    procedure_code: str | None
    billed_amount: Money
    claim_count: int
    claim_ids: list[str]


class Claim(_Model):
    claim_id: str
    patient_id: str
    provider_id: str
    service_date: date
    submission_date: date
    procedure_code: str | None
    diagnosis_code: str | None
    claim_type: str | None
    billed_amount: Money
    approved_amount: Money | None
    paid_amount: Money | None
    unpaid_amount: Money | None
    claim_status: str
    denial_reason: str | None
    insurance_plan: str | None
    processing_days: int | None
    is_approved: bool | None
    is_denied: bool | None
    high_value_flag: bool | None
    late_submission_flag: bool | None
    duplicate_flag: bool | None
    source_filename: str | None
    source_row_number: int | None
    pipeline_run_id: str | None
    created_at: datetime | None
    updated_at: datetime | None


class FilterOptions(_Model):
    claim_types: list[str]
    insurance_plans: list[str]
    claim_statuses: list[str]
    min_service_date: date | None
    max_service_date: date | None


class PipelineRun(_Model):
    pipeline_run_id: str
    pipeline_name: str | None
    run_status: str | None
    run_start_time: datetime | None
    run_end_time: datetime | None
    duration_seconds: float | None
    files_received: int | None
    files_rejected: int | None
    files_skipped: int | None
    records_received: int | None
    records_loaded: int | None
    records_rejected: int | None
    records_flagged_duplicate: int | None
    records_inserted: int | None
    records_updated: int | None
    records_unchanged: int | None
    error_message: str | None


class SourceFileInfo(_Model):
    filename: str
    sha256: str
    size_bytes: int | None
    row_count: int | None
    status: str
    error_message: str | None
    archived_path: str | None
    recorded_at: datetime | None


class RejectionCount(_Model):
    rejection_code: str
    record_count: int


class AlertRecord(_Model):
    alert_id: int
    pipeline_run_id: str | None
    alert_code: str
    severity: str
    title: str
    message: str | None
    details: dict[str, Any] | None
    channels_delivered: list[str] | None
    created_at: datetime


class PipelineRunDetail(PipelineRun):
    files: list[SourceFileInfo]
    rejection_reasons: list[RejectionCount]
    alerts: list[AlertRecord]


class RejectedRecord(_Model):
    rejection_id: int
    claim_id: str | None
    rejection_reason: str
    source_filename: str | None
    source_row_number: int | None
    pipeline_run_id: str | None
    rejected_at: datetime | None
    raw_record: dict[str, Any] | None


class ValidationRuleInfo(_Model):
    code: str
    type: str
    severity: str
    enabled: bool
    description: str


class ClaimLineage(_Model):
    """Every trace of one claim, from source record to fact row."""

    claim_id: str
    fact: Claim | None
    history: list[dict[str, Any]]
    staging: list[dict[str, Any]]
    rejections: list[dict[str, Any]]
    runs: list[dict[str, Any]]
    source_files: list[dict[str, Any]]
