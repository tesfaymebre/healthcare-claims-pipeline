"""
Data Ingestion Module

Responsibilities:
1. Discover CSV files
2. Read CSV files
3. Validate schema
4. Add ingestion metadata
5. Combine into one DataFrame
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid

import pandas as pd

from config import get_settings



settings = get_settings()


class IngestionError(Exception):
    """Base exception for ingestion errors."""


class EmptyFileError(IngestionError):
    """Raised when a CSV file contains no records."""


class SchemaValidationError(IngestionError):
    """Raised when a CSV schema is invalid."""


class FileDiscoveryError(IngestionError):
    """Raised when no input files are found."""


def discover_csv_files() -> list[Path]:
    """
    Discover all CSV files in the incoming directory.
    """

    csv_files = sorted(settings.incoming_dir.glob("*.csv"))

    if not csv_files:
        raise FileDiscoveryError(f"No CSV files found in {settings.incoming_dir}")

    return csv_files


def validate_schema(
    dataframe: pd.DataFrame,
    filename: str,
) -> None:
    """
    Verify that every required fields exists.
    """

    missing = set(settings.required_fields) - set(dataframe.columns)

    if missing:

        raise SchemaValidationError(
            f"{filename} is missing columns: "
            f"{sorted(missing)}"
        )


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Read a single CSV file.
    """

    try:

        dataframe = pd.read_csv(file_path)

    except Exception as exc:

        raise IngestionError(
            f"Unable to read {file_path.name}"
        ) from exc

    if dataframe.empty:

        raise EmptyFileError(
            f"{file_path.name} contains no rows."
        )

    validate_schema(dataframe, file_path.name)

    return dataframe


def enrich_metadata(
    dataframe: pd.DataFrame,
    filename: str,
    run_id: str,
) -> pd.DataFrame:
    """
    Add ingestion metadata.
    """

    dataframe = dataframe.copy()

    dataframe["source_filename"] = filename

    dataframe["pipeline_run_id"] = run_id

    dataframe["ingestion_timestamp"] = (
        datetime.now().astimezone(timezone.utc)
   
    )

    return dataframe


def load_all_files() -> pd.DataFrame:
    """
    Read every CSV file from the incoming directory
    and combine them into one DataFrame.
    """

    run_id = uuid.uuid4().hex

    dataframes = []

    files = discover_csv_files()

    print("-" * 60)
    print(settings.pipeline_name)
    print("-" * 60)
    print(settings.pipeline_version)
    print(run_id)
    print()

    for file in files:

        try:

            print(f"Reading {file.name}...")

            dataframe = read_csv_file(file)

            dataframe = enrich_metadata(
                dataframe,
                file.name,
                run_id,
            )

            dataframes.append(dataframe)

            print(
                f"- {len(dataframe):,} records loaded"
            )

        except IngestionError as exc:

            print(f"x {exc}")

    if not dataframes:

        raise IngestionError(
            "No valid files were loaded."
        )

    combined = pd.concat(
        dataframes,
        ignore_index=True,
    )

    print()
    print("-" * 60)
    print(
        f"Successfully loaded "
        f"{len(combined):,} records "
        f"from {len(dataframes)} file(s)."
    )
    print("-" * 60)

    return combined


if __name__ == "__main__":

    dataframe = load_all_files()

    print()

    print(dataframe.head())