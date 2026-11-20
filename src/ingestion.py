"""
Data Ingestion Module

Responsibilities:
1. Discover CSV files
2. Fingerprint each file (SHA-256) for file-level lineage and duplicate control
3. Read CSV files losslessly (every value as text)
4. Validate the file against the source contract
5. Add row-level lineage metadata
6. Combine accepted files into one DataFrame, reporting rejected files
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import Settings, get_settings


logger = logging.getLogger("healthcare_claims_pipeline.ingestion")


class IngestionError(Exception):
    """Base exception for ingestion errors."""


class EmptyFileError(IngestionError):
    """Raised when a CSV file contains no records."""


class SchemaValidationError(IngestionError):
    """Raised when a CSV schema is invalid."""


class FileDiscoveryError(IngestionError):
    """Raised when no input files are found."""


# File status values recorded in source_files.
FILE_ACCEPTED = "ACCEPTED"      # read and passed the schema check
FILE_REJECTED = "REJECTED"      # empty, unreadable or wrong schema
FILE_SKIPPED = "SKIPPED"        # identical content was already loaded
FILE_LOADED = "LOADED"          # rows committed to the database
FILE_ARCHIVED = "ARCHIVED"      # moved to data/archive after commit


@dataclass
class SourceFile:
    """
    One input file and what happened to it.

    Kept as plain data so it can cross an Airflow XCom boundary.
    """

    path: str
    filename: str
    sha256: str
    size_bytes: int
    status: str = FILE_ACCEPTED
    row_count: int = 0
    error: str | None = None
    archived_path: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> SourceFile:
        return cls(**data)


@dataclass
class IngestionResult:
    dataframe: pd.DataFrame
    files: list[SourceFile] = field(default_factory=list)

    @property
    def accepted_files(self) -> list[SourceFile]:
        return [file for file in self.files if file.status == FILE_ACCEPTED]

    @property
    def rejected_files(self) -> list[SourceFile]:
        return [file for file in self.files if file.status == FILE_REJECTED]


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """Content fingerprint: identical bytes give an identical hash, whatever the name."""

    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)

    return digest.hexdigest()


def describe_file(path: Path) -> SourceFile:
    return SourceFile(
        path=str(path),
        filename=path.name,
        sha256=file_sha256(path),
        size_bytes=path.stat().st_size,
    )


def discover_csv_files(incoming_dir: Path | None = None) -> list[Path]:
    """
    Discover all CSV files in the incoming directory.
    """

    incoming_dir = incoming_dir or get_settings().incoming_dir

    csv_files = sorted(incoming_dir.glob("*.csv"))

    if not csv_files:
        raise FileDiscoveryError(f"No CSV files found in {incoming_dir}")

    return csv_files


def validate_schema(
    dataframe: pd.DataFrame,
    filename: str,
    settings: Settings | None = None,
) -> None:
    """
    Verify the file carries every column of the source contract.

    Checking the full contract here, not only the required fields, means a
    file without e.g. denial_reason is rejected as a file with a clear
    message, instead of failing later inside a rule.
    """

    settings = settings or get_settings()

    missing = set(settings.source_columns) - set(dataframe.columns)

    if missing:

        raise SchemaValidationError(
            f"{filename} is missing columns: "
            f"{sorted(missing)}"
        )


def read_csv_file(
    file_path: Path,
    settings: Settings | None = None,
) -> pd.DataFrame:
    """
    Read a single CSV file.

    Every column is read as text. The raw layer should not reinterpret the
    source: type inference would turn a procedure code "00123" into 123 and
    "500.10" into 500.1, and the rejected-record copy would no longer be the
    original. Conversion happens in validation and transformation.
    """

    try:

        dataframe = pd.read_csv(file_path, dtype=str, keep_default_na=True)

    except pd.errors.EmptyDataError as exc:

        raise EmptyFileError(f"{file_path.name} is empty.") from exc

    except Exception as exc:

        raise IngestionError(
            f"Unable to read {file_path.name}: {exc}"
        ) from exc

    if dataframe.empty:

        raise EmptyFileError(
            f"{file_path.name} contains no rows."
        )

    validate_schema(dataframe, file_path.name, settings)

    return dataframe


def enrich_metadata(
    dataframe: pd.DataFrame,
    filename: str,
    run_id: str,
) -> pd.DataFrame:
    """
    Add ingestion metadata.

    source_row_number is the 1-based position of the record among the file's
    data rows (the header is not counted), so any staged, rejected or fact
    row can be traced back to the exact record it came from.
    """

    dataframe = dataframe.copy()

    dataframe["source_filename"] = filename

    dataframe["source_row_number"] = range(1, len(dataframe) + 1)

    dataframe["pipeline_run_id"] = run_id

    dataframe["ingestion_timestamp"] = datetime.now(timezone.utc)

    return dataframe


def ingest_files(
    files: list[SourceFile],
    run_id: str,
    settings: Settings | None = None,
) -> IngestionResult:
    """
    Read the given files and combine the accepted ones.

    A bad file is marked REJECTED with its reason and does not stop the
    others. Raises IngestionError only when no file could be used at all.
    """

    settings = settings or get_settings()

    dataframes = []

    for source_file in files:

        if source_file.status != FILE_ACCEPTED:
            continue

        try:

            dataframe = read_csv_file(Path(source_file.path), settings)

        except IngestionError as exc:

            source_file.status = FILE_REJECTED
            source_file.error = str(exc)
            logger.warning("Rejected file %s: %s", source_file.filename, exc)
            continue

        source_file.row_count = len(dataframe)

        dataframes.append(
            enrich_metadata(dataframe, source_file.filename, run_id)
        )

        logger.info(
            "Read %s: %s records (sha256 %s)",
            source_file.filename,
            f"{len(dataframe):,}",
            source_file.sha256[:12],
        )

    if not dataframes:

        raise IngestionError(
            "No valid files were loaded: "
            + "; ".join(f"{f.filename}: {f.error}" for f in files if f.error)
        )

    combined = pd.concat(dataframes, ignore_index=True)

    logger.info(
        "Ingested %s records from %d file(s)",
        f"{len(combined):,}",
        len(dataframes),
    )

    return IngestionResult(dataframe=combined, files=files)


def load_all_files(
    run_id: str | None = None,
) -> pd.DataFrame:
    """
    Read every CSV file from the incoming directory
    and combine them into one DataFrame.

    Kept for the module-level scripts and ad-hoc use; the pipeline itself
    calls ingest_files() with an explicit file list so the files it archives
    are exactly the files it loaded.
    """

    run_id = run_id or uuid.uuid4().hex

    files = [describe_file(path) for path in discover_csv_files()]

    return ingest_files(files, run_id).dataframe


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    dataframe = load_all_files()

    print()

    print(dataframe.head())
