import pandas as pd

from src.database import get_engine
from src.loading import load_claims
import pytest

pytestmark = pytest.mark.db

from src.database import transaction


TEST_CLAIM_IDS = ("TEST_CLAIM_001", "ROLLBACK_TEST")

TEST_RUN_IDS = ("test-run-001", "test-run-002")


def _delete_test_rows():

    with transaction() as connection:

        connection.exec_driver_sql(
            "DELETE FROM fact_claims WHERE claim_id = ANY(%s)",
            (list(TEST_CLAIM_IDS),),
        )

        connection.exec_driver_sql(
            "DELETE FROM stg_healthcare_claims WHERE pipeline_run_id = ANY(%s)",
            (list(TEST_RUN_IDS),),
        )

        connection.exec_driver_sql(
            "DELETE FROM rejected_claims WHERE pipeline_run_id = ANY(%s)",
            (list(TEST_RUN_IDS),),
        )


@pytest.fixture(autouse=True)
def clean_test_rows():
    """Keep the suite rerunnable: these tests assert on insert counts."""

    _delete_test_rows()

    yield

    _delete_test_rows()


def test_rerunning_same_data_does_not_create_duplicates():

    dataframe = pd.DataFrame(
        [
            {
                "claim_id": "TEST_CLAIM_001",
                "patient_id": "TEST_PATIENT_001",
                "provider_id": "TEST_PROVIDER_001",
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
                "source_filename": "test.csv",
                "pipeline_run_id": "test-run-001",
                "source_created_at": "2026-01-01T10:00:00",
            }
        ]
    )

    rejected = pd.DataFrame()

    first_result = load_claims(
        staging_frame=dataframe,
        rejected_frame=rejected,
        run_id="test-run-001",
    )

    second_result = load_claims(
        staging_frame=dataframe,
        rejected_frame=rejected,
        run_id="test-run-002",
    )

    assert first_result.inserted == 1
    assert second_result.inserted == 0
    assert second_result.unchanged == 1

    engine = get_engine()

    with engine.connect() as connection:
        count = connection.exec_driver_sql(
            """
            SELECT COUNT(*)
            FROM fact_claims
            WHERE claim_id = 'TEST_CLAIM_001'
            """
        ).scalar_one()

    assert count == 1


def test_transaction_rolls_back_on_failure():

    with pytest.raises(Exception):

        with transaction() as connection:

            connection.exec_driver_sql(
                """
                INSERT INTO fact_claims (
                    claim_id,
                    patient_id,
                    provider_id,
                    service_date,
                    submission_date,
                    billed_amount,
                    claim_status
                )
                VALUES (
                    'ROLLBACK_TEST',
                    'PATIENT_TEST',
                    'PROVIDER_TEST',
                    DATE '2026-01-01',
                    DATE '2026-01-05',
                    100.00,
                    'INVALID_STATUS'
                )
                """
            )

    engine = get_engine()

    with engine.connect() as connection:

        count = connection.exec_driver_sql(
            """
            SELECT COUNT(*)
            FROM fact_claims
            WHERE claim_id = 'ROLLBACK_TEST'
            """
        ).scalar_one()

    assert count == 0