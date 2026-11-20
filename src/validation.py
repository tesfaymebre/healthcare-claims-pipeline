"""
Data validation module for the healthcare claims pipeline.

The rules themselves are configuration: config/validation_rules.yaml, parsed
and evaluated by src/rules.py. This module orchestrates them:

1. Check that every column a rule refers to exists.
2. Evaluate every enabled rule as a vectorized mask.
3. Combine reject-rule hits into a pipe-separated rejection reason.
4. Count warn-rule hits without rejecting the row.
5. Report possible duplicates within the current batch (flag, never reject).
6. Separate valid and rejected records, keeping lineage columns intact.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from .config import Settings, get_settings
from .rules import RuleSet, evaluate_rule, get_rule_set


class ValidationError(Exception):
    """Base exception for validation errors."""


class ValidationResult:
    """
    Container for the result of validating a DataFrame.
    """

    def __init__(
        self,
        valid_records: pd.DataFrame,
        rejected_records: pd.DataFrame,
        duplicate_records: pd.DataFrame,
        summary: dict[str, Any],
    ) -> None:
        self.valid_records = valid_records
        self.rejected_records = rejected_records
        self.duplicate_records = duplicate_records
        self.summary = summary


def _check_columns(
    dataframe: pd.DataFrame,
    rule_set: RuleSet,
    settings: Settings,
) -> None:
    """Fail fast, with every missing column named, before evaluating rules."""

    needed = rule_set.referenced_columns(settings) | set(settings.duplicate_key_fields)

    missing = sorted(needed - set(dataframe.columns))

    if missing:
        raise ValidationError(
            "Columns referenced by validation rules are missing: "
            + ", ".join(missing)
        )


def _evaluate(
    dataframe: pd.DataFrame,
    rule_set: RuleSet,
    settings: Settings,
) -> tuple[dict[str, pd.Series], dict[str, pd.Series]]:
    """
    Evaluate every active rule.

    Returns reject masks and warn masks keyed by code, in rule-file order.
    Rules sharing a code are OR-ed, so a row carries each code at most once.
    """

    reject: dict[str, pd.Series] = {}
    warn: dict[str, pd.Series] = {}

    for rule in rule_set.active_rules:

        mask = evaluate_rule(dataframe, rule, settings)

        target = reject if rule.severity == "reject" else warn

        target[rule.code] = target[rule.code] | mask if rule.code in target else mask

    return reject, warn


def _join_codes(
    masks: dict[str, pd.Series],
    index: pd.Index,
) -> pd.Series:
    """
    Build "CODE_A|CODE_B" per row, vectorized. Rows with no hits get None.
    """

    joined = pd.Series("", index=index, dtype=object)

    for code, mask in masks.items():
        joined = joined.where(~mask, joined + "|" + code)

    joined = joined.str.lstrip("|")

    return joined.where(joined.ne(""), None)


def detect_duplicates(
    dataframe: pd.DataFrame,
    settings: Settings,
) -> pd.Series:
    """
    Flag possible duplicates inside the current batch.

    The key comes from schema.duplicate_key_fields. Possible duplicates are
    flagged rather than rejected: without a rule naming the authoritative
    record, dropping either copy could lose a legitimate claim. The flag that
    reaches fact_claims is recomputed in SQL against all loaded history.
    """

    return dataframe.duplicated(
        subset=settings.duplicate_key_fields,
        keep=False,
    )


def validate_claims(
    dataframe: pd.DataFrame,
    settings: Settings | None = None,
    rule_set: RuleSet | None = None,
) -> ValidationResult:
    """
    Run all configured validation rules.
    """

    settings = settings or get_settings()

    rule_set = rule_set or get_rule_set(settings.validation_rules_file)

    if dataframe.empty:
        raise ValidationError(
            "Cannot validate an empty DataFrame."
        )

    _check_columns(dataframe, rule_set, settings)

    dataframe = dataframe.copy()

    reject_masks, warn_masks = _evaluate(dataframe, rule_set, settings)

    dataframe["batch_duplicate_flag"] = detect_duplicates(dataframe, settings)

    reasons = _join_codes(reject_masks, dataframe.index)

    has_errors = reasons.notna()

    rejected_records = dataframe.loc[has_errors].copy()
    rejected_records["rejection_reason"] = reasons[has_errors]

    valid_records = dataframe.loc[~has_errors].copy()

    duplicate_records = valid_records.loc[
        valid_records["batch_duplicate_flag"]
    ].copy()

    # Warnings only matter on rows that survive; a rejected row's warnings
    # would double-count it in the quality summary.
    warning_counts = {
        code: int((mask & ~has_errors).sum())
        for code, mask in warn_masks.items()
    }

    rule_hits = {
        code: int(mask.sum())
        for code, mask in reject_masks.items()
    }

    by_file = (
        dataframe.assign(_rejected=has_errors)
        .groupby("source_filename", dropna=False)["_rejected"]
        .agg(received="size", rejected="sum")
        if "source_filename" in dataframe.columns
        else pd.DataFrame(columns=["received", "rejected"])
    )

    summary = {
        "records_received": len(dataframe),
        "records_valid": len(valid_records),
        "records_rejected": len(rejected_records),
        "records_flagged_duplicate": int(dataframe["batch_duplicate_flag"].sum()),
        "records_with_warnings": int(
            pd.concat(warn_masks.values(), axis=1).any(axis=1).loc[~has_errors].sum()
        )
        if warn_masks
        else 0,
        "rule_hits": rule_hits,
        "warning_hits": warning_counts,
        "by_file": {
            str(name): {"received": int(row.received), "rejected": int(row.rejected)}
            for name, row in by_file.iterrows()
        },
        "rules_version": rule_set.version,
        "validation_timestamp": datetime.now(UTC),
    }

    return ValidationResult(
        valid_records=valid_records,
        rejected_records=rejected_records,
        duplicate_records=duplicate_records,
        summary=summary,
    )


def print_validation_summary(
    result: ValidationResult,
) -> None:
    """
    Print a simple validation summary.
    """

    summary = result.summary

    print()
    print("=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Records received:        {summary['records_received']:,}")
    print(f"Records valid:           {summary['records_valid']:,}")
    print(f"Records rejected:        {summary['records_rejected']:,}")
    print(
        f"Possible duplicates:     "
        f"{summary['records_flagged_duplicate']:,}"
        f"  (within this batch only)"
    )
    print(f"Records with warnings:   {summary['records_with_warnings']:,}")

    if summary["rule_hits"]:
        print("-" * 60)
        for code, hits in sorted(
            summary["rule_hits"].items(), key=lambda item: -item[1]
        ):
            if hits:
                print(f"  {code:<32}{hits:>8,}")

    print("=" * 60)


if __name__ == "__main__":

    from .ingestion import load_all_files

    dataframe = load_all_files()

    result = validate_claims(dataframe)

    print_validation_summary(result)

    print("\nRejected records:")
    print(result.rejected_records[["claim_id", "rejection_reason"]].head(10))
