from pathlib import Path

import pandas as pd
import pytest

from src.ingestion import IngestionError, read_csv_file


def test_missing_file_is_rejected():

    with pytest.raises((FileNotFoundError, IngestionError)):
        read_csv_file(Path("does-not-exist.csv"))


def test_bad_schema_is_rejected(tmp_path):

    file_path = tmp_path / "bad.csv"

    dataframe = pd.DataFrame(
        {
            "claim_id": ["CLM001"],
            "patient_id": ["PAT001"],
        }
    )

    dataframe.to_csv(file_path, index=False)

    with pytest.raises(Exception):
        read_csv_file(file_path)