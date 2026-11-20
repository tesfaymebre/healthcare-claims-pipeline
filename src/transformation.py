"""
Transformation module for the healthcare claims pipeline.

This module prepares validated records for the staging table. Its scope is
deliberately narrow:

1. Trim text and turn blank strings into NULL.
2. Parse dates and timestamps.
3. Convert monetary values to fixed-scale decimals.
4. Project the frame onto the staging contract.
"""

from __future__ import annotations

import pandas as pd

from .ingestion import load_all_files
from .validation import validate_claims, print_validation_summary


class TransformationError(Exception):
    """Base exception for transformation errors."""


# Mirrors the column list of stg_healthcare_claims in sql/create_tables.sql.
STAGING_COLUMNS: list[str] = [
    "claim_id",
    "patient_id",
    "provider_id",
    "service_date",
    "submission_date",
    "procedure_code",
    "diagnosis_code",
    "claim_type",
    "billed_amount",
    "approved_amount",
    "paid_amount",
    "claim_status",
    "denial_reason",
    "insurance_plan",
    "source_filename",
    "source_created_at",
    "pipeline_run_id",
]

TEXT_COLUMNS: list[str] = [
    "claim_id",
    "patient_id",
    "provider_id",
    "procedure_code",
    "diagnosis_code",
    "claim_type",
    "claim_status",
    "denial_reason",
    "insurance_plan",
    "source_filename",
    "pipeline_run_id",
]

DATE_COLUMNS: list[str] = [
    "service_date",
    "submission_date",
]

AMOUNT_COLUMNS: list[str] = [
    "billed_amount",
    "approved_amount",
    "paid_amount",
]

TIMESTAMP_COLUMNS: list[str] = [
    "source_created_at",
]


def _to_object_with_none(
    series: pd.Series,
    valid_mask: pd.Series,
) -> pd.Series:
    """
    Convert a series to plain Python objects, using None for missing values.
    """

    return series.astype(object).where(valid_mask, None)


def standardize_text(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Trim surrounding whitespace and turn blank strings into NULL.
    """

    dataframe = dataframe.copy()

    for column in TEXT_COLUMNS:

        if column not in dataframe.columns:
            continue

        cleaned = (
            dataframe[column]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

        dataframe[column] = _to_object_with_none(
            cleaned,
            cleaned.notna(),
        )

    return dataframe


def convert_dates(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Parse date columns into Python date objects.
    """

    dataframe = dataframe.copy()

    for column in DATE_COLUMNS:

        if column not in dataframe.columns:
            continue

        parsed = pd.to_datetime(
            dataframe[column],
            errors="coerce",
        )

        dataframe[column] = _to_object_with_none(
            parsed.dt.date,
            parsed.notna(),
        )

    return dataframe


def convert_timestamps(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Parse timestamp columns into Python datetime objects.
    """

    dataframe = dataframe.copy()

    for column in TIMESTAMP_COLUMNS:

        if column not in dataframe.columns:
            continue

        parsed = pd.to_datetime(
            dataframe[column],
            errors="coerce",
        )

        dataframe[column] = _to_object_with_none(
            parsed,
            parsed.notna(),
        )

    return dataframe


def convert_amounts(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert monetary columns to Decimal at the scale of NUMERIC(12,2).

    Decimal rather than float, because binary floats cannot represent every
    two-decimal value exactly.
    """

    dataframe = dataframe.copy()

    for column in AMOUNT_COLUMNS:

        if column not in dataframe.columns:
            continue

        numeric = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

        dataframe[column] = _to_object_with_none(
            numeric,
            numeric.notna(),
        )

    return dataframe


def project_to_staging(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return only the columns the staging table accepts, in table order.
    """

    missing = [
        column
        for column in STAGING_COLUMNS
        if column not in dataframe.columns
    ]

    if missing:
        raise TransformationError(
            "Staging columns missing after transformation: "
            + ", ".join(missing)
        )

    return dataframe[STAGING_COLUMNS].copy()


def transform_claims(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Turn validated records into a staging-ready frame.
    """

    if dataframe.empty:
        return pd.DataFrame(columns=STAGING_COLUMNS)

    dataframe = dataframe.copy()

    dataframe = standardize_text(dataframe)

    dataframe = convert_dates(dataframe)

    dataframe = convert_timestamps(dataframe)

    dataframe = convert_amounts(dataframe)

    return project_to_staging(dataframe)


if __name__ == "__main__":
    
    frame = load_all_files()

    result = validate_claims(frame)

    print_validation_summary(result)

    staging_frame = transform_claims(result.valid_records)

    print("\nStaging-ready records:")

    print(staging_frame.head())

    print("\nDtypes:")

    print(staging_frame.dtypes)
