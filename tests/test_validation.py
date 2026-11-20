import pandas as pd

from src.validation import validate_claims


def make_valid_claim():
    return {
        "claim_id": "CLM001",
        "patient_id": "PAT001",
        "provider_id": "PRV001",
        "service_date": "2026-01-01",
        "submission_date": "2026-01-05",
        "procedure_code": "80053",
        "diagnosis_code": "D001",
        "claim_type": "Medical",
        "billed_amount": 500.00,
        "approved_amount": 450.00,
        "paid_amount": 400.00,
        "claim_status": "Approved",
        "denial_reason": None,
        "insurance_plan": "PlanA",
        "source_created_at": "2026-01-01T10:00:00",
        "source_filename": "test.csv",
        "pipeline_run_id": "test-run",
        "ingestion_timestamp": "2026-01-01T10:00:00",
    }


def test_valid_claim_is_accepted():
    dataframe = pd.DataFrame([make_valid_claim()])

    result = validate_claims(dataframe)

    assert len(result.valid_records) == 1
    assert len(result.rejected_records) == 0


def test_missing_required_field_is_rejected():
    record = make_valid_claim()
    record["patient_id"] = None

    dataframe = pd.DataFrame([record])

    result = validate_claims(dataframe)

    assert len(result.rejected_records) == 1


def test_negative_billed_amount_is_rejected():
    record = make_valid_claim()
    record["billed_amount"] = -100

    dataframe = pd.DataFrame([record])

    result = validate_claims(dataframe)

    assert len(result.rejected_records) == 1


def test_submission_before_service_is_rejected():
    record = make_valid_claim()

    record["service_date"] = "2026-02-01"
    record["submission_date"] = "2026-01-01"

    dataframe = pd.DataFrame([record])

    result = validate_claims(dataframe)

    assert len(result.rejected_records) == 1


def test_paid_amount_cannot_exceed_approved_amount():
    record = make_valid_claim()

    record["approved_amount"] = 100
    record["paid_amount"] = 200

    dataframe = pd.DataFrame([record])

    result = validate_claims(dataframe)

    assert len(result.rejected_records) == 1