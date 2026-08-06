from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


RANDOM_SEED = 42

VALID_STATUSES = [
    "Submitted",
    "Pending",
    "Approved",
    "Denied",
    "Paid",
]

CLAIM_TYPES = [
    "Medical",
    "Dental",
    "Pharmacy",
    "Vision",
]

INSURANCE_PLANS = [
    "Bronze",
    "Silver",
    "Gold",
    "Platinum",
]

DENIAL_REASONS = [
    "Missing documentation",
    "Service not covered",
    "Duplicate claim",
    "Invalid procedure code",
    "Prior authorization required",
    "Patient not eligible",
]

PROCEDURE_CODES = [
    "99213",
    "99214",
    "80053",
    "85025",
    "93000",
    "71046",
    "36415",
    "90834",
    "97110",
    "J3490",
]

DIAGNOSIS_CODES = [
    "J06.9",
    "I10",
    "E11.9",
    "M54.5",
    "R51.9",
    "K21.9",
    "F41.9",
    "N39.0",
    "Z00.0",
    "R07.9",
]


def random_date(
    start_date: datetime,
    end_date: datetime,
) -> datetime:
    """Return a random datetime between two dates."""
    difference = end_date - start_date
    random_seconds = random.randint(0, int(difference.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)


def generate_base_claims(
    record_count: int,
) -> pd.DataFrame:
    """Generate initially valid synthetic healthcare claims."""
    today = datetime.now()
    service_start = today - timedelta(days=730)

    records: list[dict] = []

    for index in range(1, record_count + 1):
        claim_id = f"CLM{index:08d}"
        patient_id = f"PAT{random.randint(1, 5000):06d}"
        provider_id = f"PRV{random.randint(1, 500):05d}"

        service_date = random_date(service_start, today - timedelta(days=1))
        submission_delay = random.randint(0, 45)
        submission_date = service_date + timedelta(days=submission_delay)

        claim_status = random.choices(
            VALID_STATUSES,
            weights=[10, 15, 25, 20, 30],
            k=1,
        )[0]

        billed_amount = round(
            max(25, np.random.lognormal(mean=6.2, sigma=0.9)),
            2,
        )

        if claim_status == "Denied":
            approved_amount = 0.0
            paid_amount = 0.0
            denial_reason = random.choice(DENIAL_REASONS)

        elif claim_status in {"Submitted", "Pending"}:
            approved_amount = 0.0
            paid_amount = 0.0
            denial_reason = None

        elif claim_status == "Approved":
            approved_amount = round(
                billed_amount * random.uniform(0.55, 1.0),
                2,
            )
            paid_amount = 0.0
            denial_reason = None

        else:
            approved_amount = round(
                billed_amount * random.uniform(0.55, 1.0),
                2,
            )
            paid_amount = round(
                approved_amount * random.uniform(0.85, 1.0),
                2,
            )
            denial_reason = None

        records.append(
            {
                "claim_id": claim_id,
                "patient_id": patient_id,
                "provider_id": provider_id,
                "service_date": service_date.date().isoformat(),
                "submission_date": submission_date.date().isoformat(),
                "procedure_code": random.choice(PROCEDURE_CODES),
                "diagnosis_code": random.choice(DIAGNOSIS_CODES),
                "claim_type": random.choice(CLAIM_TYPES),
                "billed_amount": billed_amount,
                "approved_amount": approved_amount,
                "paid_amount": paid_amount,
                "claim_status": claim_status,
                "denial_reason": denial_reason,
                "insurance_plan": random.choice(INSURANCE_PLANS),
                "created_at": submission_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )

    return pd.DataFrame(records)


def choose_indexes(
    dataframe: pd.DataFrame,
    percentage: float,
) -> np.ndarray:
    """Select random row indexes based on a percentage."""
    count = max(1, int(len(dataframe) * percentage))
    return np.random.choice(
        dataframe.index,
        size=count,
        replace=False,
    )


def inject_missing_required_fields(
    dataframe: pd.DataFrame,
    percentage: float = 0.03,
) -> None:
    indexes = choose_indexes(dataframe, percentage)
    fields = [
        "claim_id",
        "patient_id",
        "provider_id",
        "service_date",
        "billed_amount",
    ]

    for index in indexes:
        field = random.choice(fields)
        dataframe.at[index, field] = None


def inject_invalid_dates(
    dataframe: pd.DataFrame,
    percentage: float = 0.02,
) -> None:
    indexes = choose_indexes(dataframe, percentage)

    for position, index in enumerate(indexes):
        if position % 2 == 0:
            dataframe.at[index, "service_date"] = "not-a-date"
        else:
            service_date = datetime.now().date()
            submission_date = service_date - timedelta(days=20)

            dataframe.at[index, "service_date"] = service_date.isoformat()
            dataframe.at[
                index,
                "submission_date",
            ] = submission_date.isoformat()


def inject_financial_errors(
    dataframe: pd.DataFrame,
    percentage: float = 0.02,
) -> None:
    indexes = choose_indexes(dataframe, percentage)

    for position, index in enumerate(indexes):
        billed = float(dataframe.at[index, "billed_amount"] or 100)

        error_type = position % 3

        if error_type == 0:
            dataframe.at[index, "billed_amount"] = -abs(billed)

        elif error_type == 1:
            dataframe.at[index, "approved_amount"] = billed + 500

        else:
            approved = float(
                dataframe.at[index, "approved_amount"] or billed
            )
            dataframe.at[index, "paid_amount"] = approved + 250


def inject_invalid_statuses(
    dataframe: pd.DataFrame,
    percentage: float = 0.02,
) -> None:
    indexes = choose_indexes(dataframe, percentage)
    invalid_statuses = [
        "Complete",
        "Rejected",
        "Unknown",
        "PAID_OUT",
    ]

    for index in indexes:
        dataframe.at[
            index,
            "claim_status",
        ] = random.choice(invalid_statuses)


def inject_missing_denial_reasons(
    dataframe: pd.DataFrame,
    percentage: float = 0.02,
) -> None:
    indexes = choose_indexes(dataframe, percentage)

    for index in indexes:
        dataframe.at[index, "claim_status"] = "Denied"
        dataframe.at[index, "paid_amount"] = 0.0
        dataframe.at[index, "denial_reason"] = None


def create_duplicate_claims(
    dataframe: pd.DataFrame,
    percentage: float = 0.04,
) -> pd.DataFrame:
    """
    Duplicate business information while assigning a different claim ID.

    The duplicate business key is:
    patient_id, provider_id, service_date,
    procedure_code, billed_amount.
    """
    duplicate_count = max(1, int(len(dataframe) * percentage))

    duplicates = dataframe.sample(
        n=duplicate_count,
        random_state=RANDOM_SEED,
    ).copy()

    start_number = len(dataframe) + 1

    duplicates["claim_id"] = [
        f"CLM{number:08d}"
        for number in range(
            start_number,
            start_number + duplicate_count,
        )
    ]

    return pd.concat(
        [dataframe, duplicates],
        ignore_index=True,
    )


def add_exact_duplicates(
    dataframe: pd.DataFrame,
    count: int = 20,
) -> pd.DataFrame:
    """Add a small number of completely identical rows."""
    exact_duplicates = dataframe.sample(
        n=min(count, len(dataframe)),
        random_state=RANDOM_SEED + 1,
    ).copy()

    return pd.concat(
        [dataframe, exact_duplicates],
        ignore_index=True,
    )


def split_and_write_files(
    dataframe: pd.DataFrame,
    output_directory: Path,
    file_count: int = 3,
) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)

    shuffled = dataframe.sample(
        frac=1,
        random_state=RANDOM_SEED,
    ).reset_index(drop=True)

    # Split positions rather than the frame itself: np.array_split on a
    # DataFrame returns ndarrays, which have no to_csv.
    position_groups = np.array_split(np.arange(len(shuffled)), file_count)

    for file_number, positions in enumerate(position_groups, start=1):
        part = shuffled.iloc[positions]

        output_path = (
            output_directory
            / f"healthcare_claims_part_{file_number:02d}.csv"
        )

        part.to_csv(output_path, index=False)

        print(
            f"Created {output_path} "
            f"with {len(part):,} records"
        )


def generate_data(
    record_count: int,
    output_directory: Path,
    file_count: int,
) -> None:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    dataframe = generate_base_claims(record_count)

    inject_missing_required_fields(dataframe)
    inject_invalid_dates(dataframe)
    inject_financial_errors(dataframe)
    inject_invalid_statuses(dataframe)
    inject_missing_denial_reasons(dataframe)

    dataframe = create_duplicate_claims(dataframe)
    dataframe = add_exact_duplicates(dataframe)

    split_and_write_files(
        dataframe=dataframe,
        output_directory=output_directory,
        file_count=file_count,
    )

    print("\nGeneration complete")
    print(f"Base records: {record_count:,}")
    print(f"Final records: {len(dataframe):,}")
    print(f"Output folder: {output_directory.resolve()}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic healthcare claims data."
    )

    parser.add_argument(
        "--records",
        type=int,
        default=20_000,
        help="Number of base claim records.",
    )

    parser.add_argument(
        "--files",
        type=int,
        default=3,
        help="Number of CSV files to generate.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/incoming"),
        help="Output directory.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()

    generate_data(
        record_count=arguments.records,
        output_directory=arguments.output,
        file_count=arguments.files,
    )


#pip install pandas numpy
#the following creates 20820 rows
#python generate_claims.py

#you can change the size
#python generate_claims.py --records 50000 --files 5