"""
Data validation module for the healthcare claims pipeline.

Responsibilities:
1. Validate required fields.
2. Validate dates.
3. Validate financial values.
4. Validate claim statuses and claim types.
5. Validate denial rules.
6. Detect possible duplicates.
7. Detect late submissions.
8. Separate valid and rejected records.
9. Preserve rejection reasons and lineage information.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from config import Settings, get_settings


class ValidationError(Exception):
    """Base exception for validation errors."""


class ValidationResult:
    """
    Container for the result of validating a DataFrame.
    """

    def __init__(
        self,
        valid_records: pd.DataFrame,
        rejected_records: pd.DataFrame,
        duplicate_records: pd.DataFrame,
        summary: dict[str, Any],
    ) -> None:
        self.valid_records = valid_records
        self.rejected_records = rejected_records
        self.duplicate_records = duplicate_records
        self.summary = summary


def _add_validation_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns used internally by the validation process.
    """

    dataframe = dataframe.copy()

    dataframe["_validation_errors"] = [[] for _ in range(len(dataframe))]

    dataframe["duplicate_flag"] = False

    dataframe["late_submission_flag"] = False

    return dataframe


def _add_error(
    dataframe: pd.DataFrame,
    mask: pd.Series,
    error_code: str,
) -> None:
    """
    Add a validation error code to every row matching the mask.
    Multiple errors can be stored on the same record.
    """

    indexes = dataframe.index[mask]

    for index in indexes:
        dataframe.at[index, "_validation_errors"].append(error_code)


def validate_required_fields(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> None:
    """
    A record is rejected when one or more required fields
    are missing or empty.
    """

    required_fields = settings.required_fields

    invalid_rows = pd.Series(
        False,
        index=dataframe.index,
    )

    for column in required_fields:

        missing = (
            dataframe[column].isna()
            | dataframe[column].astype(str).str.strip().eq("")
        )

        invalid_rows |= missing

    _add_error(
        dataframe,
        invalid_rows,
        "MISSING_REQUIRED_FIELD",
    )


def validate_dates(
    dataframe: pd.DataFrame,
) -> None:
    """
    Validate service_date and submission_date.

    Rules:
    1. Dates must be parseable.
    2. submission_date cannot be before service_date.
    3. Future service dates are rejected.
    """

    service_dates = pd.to_datetime(
        dataframe["service_date"],
        errors="coerce",
    )

    submission_dates = pd.to_datetime(
        dataframe["submission_date"],
        errors="coerce",
    )

    invalid_service_date = (
        dataframe["service_date"].notna()
        & service_dates.isna()
    )

    _add_error(
        dataframe,
        invalid_service_date,
        "INVALID_SERVICE_DATE",
    )

    invalid_submission_date = (
        dataframe["submission_date"].notna()
        & submission_dates.isna()
    )

    _add_error(
        dataframe,
        invalid_submission_date,
        "INVALID_SUBMISSION_DATE",
    )

    submission_before_service = (
        service_dates.notna()
        & submission_dates.notna()
        & (submission_dates < service_dates)
    )

    _add_error(
        dataframe,
        submission_before_service,
        "SUBMISSION_BEFORE_SERVICE",
    )

    today = pd.Timestamp.now(tz="UTC").normalize()

    future_service_date = (
        service_dates.notna()
        & (service_dates.dt.normalize() > today.tz_localize(None))
    )

    _add_error(
        dataframe,
        future_service_date,
        "FUTURE_SERVICE_DATE",
    )


def validate_financials(
    dataframe: pd.DataFrame,
) -> None:
    """
    Validate financial values.

    Rules:
    billed_amount > 0
    approved_amount >= 0
    paid_amount >= 0
    approved_amount <= billed_amount
    paid_amount <= approved_amount
    """

    billed = pd.to_numeric(
        dataframe["billed_amount"],
        errors="coerce",
    )

    approved = pd.to_numeric(
        dataframe["approved_amount"],
        errors="coerce",
    )

    paid = pd.to_numeric(
        dataframe["paid_amount"],
        errors="coerce",
    )

    invalid_billed = (
        billed.isna()
        | (billed <= 0)
    )

    _add_error(
        dataframe,
        invalid_billed,
        "INVALID_BILLED_AMOUNT",
    )

    invalid_approved = (
        approved.notna()
        & (approved < 0)
    )

    _add_error(
        dataframe,
        invalid_approved,
        "INVALID_APPROVED_AMOUNT",
    )

    invalid_paid = (
        paid.notna()
        & (paid < 0)
    )

    _add_error(
        dataframe,
        invalid_paid,
        "INVALID_PAID_AMOUNT",
    )

    approved_exceeds_billed = (
        billed.notna()
        & approved.notna()
        & (approved > billed)
    )

    _add_error(
        dataframe,
        approved_exceeds_billed,
        "APPROVED_EXCEEDS_BILLED",
    )

    paid_exceeds_approved = (
        paid.notna()
        & approved.notna()
        & (paid > approved)
    )

    _add_error(
        dataframe,
        paid_exceeds_approved,
        "PAID_EXCEEDS_APPROVED",
    )


def validate_statuses(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> None:
    """
    Validate claim statuses against the configured
    list of allowed statuses.
    """

    allowed_statuses = set(settings.allowed_statuses)

    invalid_status = ~dataframe["claim_status"].isin(allowed_statuses)

    _add_error(
        dataframe,
        invalid_status,
        "INVALID_CLAIM_STATUS",
    )


def validate_claim_types(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> None:
    """
    Validate claim types against the configured
    list of allowed claim types.
    """

    allowed_claim_types = set(settings.allowed_claim_types)

    invalid_claim_type = (
        dataframe["claim_type"].notna()
        & ~dataframe["claim_type"].isin(
            allowed_claim_types
        )
    )

    _add_error(
        dataframe,
        invalid_claim_type,
        "INVALID_CLAIM_TYPE",
    )


def validate_denial_rules(
    dataframe: pd.DataFrame,
) -> None:
    """
    Validate denial-specific business rules.

    Rules:
    1. A denied claim must have a denial reason.
    2. Approved/Paid claims containing a denial reason
    are not rejected because the specification describes
    this as a 'normally should not' condition.
    """

    denied_without_reason = (
        (dataframe["claim_status"] == "Denied")
        & (
            dataframe["denial_reason"].isna()
            | (dataframe["denial_reason"].astype(str).str.strip() == "")
        )
    )

    _add_error(
        dataframe,
        denied_without_reason,
        "MISSING_DENIAL_REASON",
    )


def detect_duplicates(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> None:
    """
    Detect possible duplicates.

    Duplicate key fields:
        patient_id
        provider_id
        service_date
        procedure_code
        billed_amount

    Possible duplicates are flagged rather than rejected.
    """

    duplicate_fields = settings.duplicate_key_fields

    missing_columns = [
        column
        for column in duplicate_fields
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValidationError(
            "Duplicate detection columns missing: "
            + ", ".join(missing_columns)
        )

    duplicate_mask = dataframe.duplicated(
        subset=duplicate_fields,
        keep=False,
    )

    dataframe.loc[
        duplicate_mask,
        "duplicate_flag",
    ] = True


def detect_late_submissions(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> None:
    """
    Flag claims submitted more than the configured
    number of days after the service date.

    Late submissions are flagged, not rejected.
    """

    service_dates = pd.to_datetime(
        dataframe["service_date"],
        errors="coerce",
    )

    submission_dates = pd.to_datetime(
        dataframe["submission_date"],
        errors="coerce",
    )

    processing_days = (
        submission_dates - service_dates
    ).dt.days

    late_mask = (
        processing_days.notna()
        & (processing_days > settings.late_submission_days)
    )

    dataframe.loc[
        late_mask,
        "late_submission_flag",
    ] = True


def _prepare_rejection_reason(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert the internal list of validation errors into
    a readable pipe-separated string.
    """

    dataframe["rejection_reason"] = dataframe["_validation_errors"].apply(
        lambda errors: "|".join(errors)
        if errors
        else None
    )

    return dataframe


def _remove_internal_columns(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove columns used only internally during validation.
    """

    return dataframe.drop(
        columns=["_validation_errors"],
        errors="ignore",
    )


def validate_claims(
    dataframe: pd.DataFrame,
    settings: Settings | None = None,
) -> ValidationResult:
    """
    Run all validation rules.
    """

    settings = settings or get_settings()

    if dataframe.empty:
        raise ValidationError(
            "Cannot validate an empty DataFrame."
        )

    dataframe = _add_validation_columns(
        dataframe
    )

    # Rejection-level validation

    validate_required_fields(dataframe,settings)

    validate_dates(dataframe)

    validate_financials(dataframe)

    validate_statuses(dataframe,settings)

    validate_claim_types(dataframe,settings)

    validate_denial_rules(dataframe)

    # Flagging rules

    detect_duplicates(
        dataframe,
        settings,
    )

    detect_late_submissions(
        dataframe,
        settings,
    )

    # Separate valid and rejected records

    has_errors = dataframe[
        "_validation_errors"
    ].apply(bool)

    rejected_records = dataframe.loc[
        has_errors
    ].copy()

    valid_records = dataframe.loc[
        ~has_errors
    ].copy()

    # Prepare readable rejection reason

    rejected_records = _prepare_rejection_reason(rejected_records)

    # Extract possible duplicate records

    duplicate_records = valid_records.loc[
        valid_records["duplicate_flag"]
    ].copy()

    # Remove internal validation columns

    valid_records = _remove_internal_columns(
        valid_records
    )

    rejected_records = _remove_internal_columns(
        rejected_records
    )

    duplicate_records = _remove_internal_columns(
        duplicate_records
    )

    # summary

    duplicate_count = int(
        dataframe["duplicate_flag"].sum()
    )

    late_submission_count = int(
        dataframe["late_submission_flag"].sum()
    )

    summary = {
        "records_received": len(dataframe),
        "records_valid": len(valid_records),
        "records_rejected": len(rejected_records),
        "records_flagged_duplicate": duplicate_count,
        "records_flagged_late_submission": (
            late_submission_count
        ),
        "validation_timestamp": datetime.now(UTC),
    }

    return ValidationResult(
        valid_records=valid_records,
        rejected_records=rejected_records,
        duplicate_records=duplicate_records,
        summary=summary,
    )


def print_validation_summary(
    result: ValidationResult,
) -> None:
    """
    Print a simple validation summary.
    """

    summary = result.summary

    print()
    print("=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    print(
        f"Records received:        "
        f"{summary['records_received']:,}"
    )

    print(
        f"Records valid:           "
        f"{summary['records_valid']:,}"
    )

    print(
        f"Records rejected:        "
        f"{summary['records_rejected']:,}"
    )

    print(
        f"Possible duplicates:     "
        f"{summary['records_flagged_duplicate']:,}"
    )

    print(
        f"Late submissions:        "
        f"{summary['records_flagged_late_submission']:,}"
    )

    print("=" * 60)


if __name__ == "__main__":

    from ingestion import load_all_files

    dataframe = load_all_files()

    result = validate_claims(dataframe)

    print_validation_summary(result)

    print("\nRejected records:")
    print(
        result.rejected_records[
            [
                "claim_id",
                "rejection_reason",
            ]
        ].head(10)
    )

    print("\nPossible duplicate records:")
    print(
        result.duplicate_records[
            [
                "claim_id",
                "duplicate_flag",
            ]
        ].head(10)
    )