"""
Data transformation module for the healthcare claims pipeline.

Responsibilities:
1. Standardize data types needed for downstream processing.
2. Calculate processing_days.
3. Calculate unpaid_amount.
4. Create is_approved flag.
5. Create is_denied flag.
6. Create high_value_flag.
7. Preserve duplicate_flag from validation.
"""

from __future__ import annotations

import pandas as pd

from config import Settings, get_settings


class TransformationError(Exception):
    """Base exception for transformation errors."""


def _validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
) -> None:
    """
    Make sure all columns required for transformation exist.
    """

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise TransformationError(
            "Missing columns required for transformation: "
            + ", ".join(missing_columns)
        )


def standardize_data_types(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert source columns to appropriate data types.

    Dates:
        service_date
        submission_date

    Numeric:
        billed_amount
        approved_amount
        paid_amount
    """

    dataframe = dataframe.copy()

    dataframe["service_date"] = pd.to_datetime(
        dataframe["service_date"],
        errors="raise",
    )

    dataframe["submission_date"] = pd.to_datetime(
        dataframe["submission_date"],
        errors="raise",
    )

    dataframe["billed_amount"] = pd.to_numeric(
        dataframe["billed_amount"],
        errors="raise",
    )

    dataframe["approved_amount"] = pd.to_numeric(
        dataframe["approved_amount"],
        errors="raise",
    )

    dataframe["paid_amount"] = pd.to_numeric(
        dataframe["paid_amount"],
        errors="raise",
    )

    return dataframe


def calculate_processing_days(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the number of days between service and submission.
    """

    dataframe = dataframe.copy()

    dataframe["processing_days"] = (
        dataframe["submission_date"]
        - dataframe["service_date"]
    ).dt.days

    return dataframe


def calculate_unpaid_amount(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the unpaid amount.

    unpaid_amount =
        approved_amount - paid_amount
    """

    dataframe = dataframe.copy()

    dataframe["unpaid_amount"] = (
        dataframe["approved_amount"]
        - dataframe["paid_amount"]
    )

    return dataframe


def add_approval_flag(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Set is_approved to True when the claim status
    is Approved or Paid.
    """

    dataframe = dataframe.copy()

    dataframe["is_approved"] = dataframe["claim_status"].isin(
        set(["Approved", "Paid"])
    )

    return dataframe


def add_denial_flag(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Set is_denied to True when the claim status is Denied.
    """

    dataframe = dataframe.copy()

    dataframe["is_denied"] = (dataframe["claim_status"] == "Denied")

    return dataframe


def add_high_value_flag(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    """
    Flag claims whose billed amount exceeds
    the configured high-value threshold.
    """

    dataframe = dataframe.copy()

    dataframe["high_value_flag"] = (dataframe["billed_amount"] > settings.high_value_threshold)

    return dataframe


def preserve_duplicate_flag(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ensure duplicate_flag exists.

    If transformation is called independently and the column
    does not exist, default it to False.
    """

    dataframe = dataframe.copy()

    if "duplicate_flag" not in dataframe.columns:
        dataframe["duplicate_flag"] = False

    return dataframe


def transform_claims(
    dataframe: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    """
    Transform claims data.
    """

    settings = settings or get_settings()

    if dataframe.empty:
        raise TransformationError(
            "Cannot transform an empty DataFrame."
        )

    required_columns = [
        "service_date",
        "submission_date",
        "billed_amount",
        "approved_amount",
        "paid_amount",
        "claim_status",
    ]

    _validate_required_columns(
        dataframe,
        required_columns
    )

    # Work on a copy so the validated DataFrame is not mutated.
    transformed = dataframe.copy()

    # Standardize data types
    transformed = standardize_data_types(transformed)

   # Calculate processing days
    transformed = calculate_processing_days(transformed)

    # Calculate unpaid amount
    transformed = calculate_unpaid_amount(transformed)

    # Add approval flag
    transformed = add_approval_flag(transformed)

    # Add denial flag
    transformed = add_denial_flag(transformed)

    # Add high-value flag
    transformed = add_high_value_flag(transformed, settings)

    # Preserve duplicate flag from validation
    transformed = preserve_duplicate_flag(transformed)

    return transformed


def print_transformation_summary(
    dataframe: pd.DataFrame,
) -> None:
    """
    Print a simple summary of the transformed data.
    """

    print()
    print("=" * 60)
    print("TRANSFORMATION SUMMARY")
    print("=" * 60)

    print(
        f"Records transformed: "
        f"{len(dataframe):,}"
    )

    print(
        f"Approved claims: "
        f"{int(dataframe['is_approved'].sum()):,}"
    )

    print(
        f"Denied claims: "
        f"{int(dataframe['is_denied'].sum()):,}"
    )

    print(
        f"High-value claims: "
        f"{int(dataframe['high_value_flag'].sum()):,}"
    )

    print(
        f"Possible duplicates: "
        f"{int(dataframe['duplicate_flag'].sum()):,}"
    )

    print(
        f"Total unpaid amount: "
        f"${dataframe['unpaid_amount'].sum():,.2f}"
    )

    print("=" * 60)


if __name__ == "__main__":

    from ingestion import load_all_files
    from validation import validate_claims

    # Load all files
    raw_dataframe = load_all_files()

    # Validate claims
    validation_result = validate_claims(raw_dataframe)

    # Transform claims
    transformed_dataframe = transform_claims(validation_result.valid_records)

    print_transformation_summary(transformed_dataframe)

    print("\nTransformed records:")

    print(
        transformed_dataframe[
            [
                "claim_id",
                "billed_amount",
                "approved_amount",
                "paid_amount",
                "processing_days",
                "unpaid_amount",
                "is_approved",
                "is_denied",
                "high_value_flag",
                "duplicate_flag",
            ]
        ].head(10)
    )