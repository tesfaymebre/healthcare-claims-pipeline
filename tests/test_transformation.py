import pandas as pd

from src.transformation import transform_claims


def test_text_is_trimmed():
    dataframe = pd.DataFrame(
        [
            {
                "claim_id": " CLM001 ",
                "patient_id": " PAT001 ",
                "provider_id": " PRV001 ",
                "service_date": "2026-01-01",
                "submission_date": "2026-01-05",
                "procedure_code": "80053",
                "diagnosis_code": "D001",
                "claim_type": "Medical",
                "billed_amount": "500.00",
                "approved_amount": "450.00",
                "paid_amount": "400.00",
                "claim_status": "Approved",
                "denial_reason": None,
                "insurance_plan": "PlanA",
                "source_created_at": "2026-01-01T10:00:00",
                "source_filename": "test.csv",
                "pipeline_run_id": "run-001",
            }
        ]
    )

    result = transform_claims(dataframe)

    assert result.iloc[0]["claim_id"] == "CLM001"
    assert result.iloc[0]["patient_id"] == "PAT001"


def test_dates_are_converted():
    dataframe = pd.DataFrame(
        [
            {
                "claim_id": "CLM001",
                "patient_id": "PAT001",
                "provider_id": "PRV001",
                "service_date": "2026-01-01",
                "submission_date": "2026-01-05",
                "procedure_code": "80053",
                "diagnosis_code": "D001",
                "claim_type": "Medical",
                "billed_amount": "500.00",
                "approved_amount": "450.00",
                "paid_amount": "400.00",
                "claim_status": "Approved",
                "denial_reason": None,
                "insurance_plan": "PlanA",
                "source_created_at": "2026-01-01T10:00:00",
                "source_filename": "test.csv",
                "pipeline_run_id": "run-001",
            }
        ]
    )

    result = transform_claims(dataframe)

    assert str(result.iloc[0]["service_date"]) == "2026-01-01"
    assert str(result.iloc[0]["submission_date"]) == "2026-01-05"


def test_amounts_are_numeric():
    dataframe = pd.DataFrame(
        [
            {
                "claim_id": "CLM001",
                "patient_id": "PAT001",
                "provider_id": "PRV001",
                "service_date": "2026-01-01",
                "submission_date": "2026-01-05",
                "procedure_code": "80053",
                "diagnosis_code": "D001",
                "claim_type": "Medical",
                "billed_amount": "500.00",
                "approved_amount": "450.00",
                "paid_amount": "400.00",
                "claim_status": "Approved",
                "denial_reason": None,
                "insurance_plan": "PlanA",
                "source_created_at": "2026-01-01T10:00:00",
                "source_filename": "test.csv",
                "pipeline_run_id": "run-001",
            }
        ]
    )

    result = transform_claims(dataframe)

    assert result.iloc[0]["billed_amount"] == 500.0
    assert result.iloc[0]["approved_amount"] == 450.0
    assert result.iloc[0]["paid_amount"] == 400.0