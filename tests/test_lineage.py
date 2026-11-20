"""Unit tests for file fingerprints, row lineage and OpenLineage events (no database)."""

import json
import uuid

import pandas as pd
import pytest

from src.ingestion import (
    FILE_ACCEPTED,
    FILE_REJECTED,
    EmptyFileError,
    describe_file,
    ingest_files,
    read_csv_file,
)
from src.lineage import LineageEmitter, _openlineage_run_id
from tests.test_validation import make_valid_claim


SOURCE_ONLY = [
    key for key in make_valid_claim()
    if key not in ("source_filename", "pipeline_run_id", "ingestion_timestamp")
]


def write_claims(path, count=2, **overrides):
    records = []
    for index in range(count):
        record = {key: make_valid_claim()[key] for key in SOURCE_ONLY}
        record["claim_id"] = f"CLM{index:03d}"
        record.update(overrides)
        records.append(record)
    pd.DataFrame(records).to_csv(path, index=False)
    return path


def test_checksum_depends_on_content_not_name(tmp_path):
    first = write_claims(tmp_path / "a.csv")
    second = tmp_path / "renamed.csv"
    second.write_bytes(first.read_bytes())

    assert describe_file(first).sha256 == describe_file(second).sha256


def test_rows_are_numbered_and_values_kept_as_text(tmp_path, settings):
    path = write_claims(tmp_path / "a.csv", count=3, procedure_code="00123")

    result = ingest_files([describe_file(path)], "run-1", settings)

    assert result.dataframe["source_row_number"].tolist() == [1, 2, 3]
    assert result.dataframe["procedure_code"].iloc[0] == "00123"
    assert result.files[0].row_count == 3


def test_bad_file_is_rejected_without_stopping_good_ones(tmp_path, settings):
    good = write_claims(tmp_path / "good.csv")
    bad = tmp_path / "bad.csv"
    pd.DataFrame({"claim_id": ["X"]}).to_csv(bad, index=False)

    result = ingest_files([describe_file(good), describe_file(bad)], "run-1", settings)

    statuses = {file.filename: file.status for file in result.files}
    assert statuses == {"good.csv": FILE_ACCEPTED, "bad.csv": FILE_REJECTED}
    assert "missing columns" in result.rejected_files[0].error
    assert len(result.dataframe) == 2


def test_zero_byte_file_is_an_empty_file(tmp_path, settings):
    path = tmp_path / "empty.csv"
    path.write_text("")
    with pytest.raises(EmptyFileError):
        read_csv_file(path, settings)


def test_openlineage_run_id_is_a_stable_uuid():
    hex_id = uuid.uuid4().hex
    assert _openlineage_run_id(hex_id) == str(uuid.UUID(hex=hex_id))
    assert _openlineage_run_id("test-run") == _openlineage_run_id("test-run")
    uuid.UUID(_openlineage_run_id("test-run"))


def test_complete_event_describes_inputs_and_outputs(tmp_path, settings):
    file = describe_file(write_claims(tmp_path / "a.csv"))
    file.row_count = 2

    event = LineageEmitter(settings).build_event(
        "COMPLETE", "run-1", [file], output_row_counts={"fact_claims": 2}
    )

    assert event["eventType"] == "COMPLETE"
    assert event["job"]["name"] == settings.pipeline_name
    assert event["inputs"][0]["inputFacets"]["dataQualityMetrics"]["rowCount"] == 2
    outputs = {o["name"].split(".")[-1]: o for o in event["outputs"]}
    assert outputs["fact_claims"]["outputFacets"]["outputStatistics"]["rowCount"] == 2


def test_fail_event_carries_the_error(settings):
    event = LineageEmitter(settings).build_event("FAIL", "run-1", error="boom")
    assert event["run"]["facets"]["errorMessage"]["message"] == "boom"


def test_file_transport_appends_json_lines(isolated_settings):
    isolated_settings.lineage_transport = "file"
    emitter = LineageEmitter(isolated_settings)

    emitter.emit(emitter.build_event("START", "run-1"))
    emitter.emit(emitter.build_event("COMPLETE", "run-1", output_row_counts={}))

    lines = next(isolated_settings.lineage_dir.glob("*.jsonl")).read_text().splitlines()
    assert [json.loads(line)["eventType"] for line in lines] == ["START", "COMPLETE"]


def test_http_failure_falls_back_to_file(isolated_settings):
    isolated_settings.lineage_transport = "http"
    isolated_settings.openlineage_url = "http://127.0.0.1:9"  # nothing listens here

    emitter = LineageEmitter(isolated_settings)
    emitter.emit(emitter.build_event("START", "run-1"))

    assert list(isolated_settings.lineage_dir.glob("*.jsonl"))
