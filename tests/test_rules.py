"""Unit tests for the configuration-driven rule engine (no database)."""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from src.rules import RuleConfigError, RuleSet, evaluate_rule, load_rule_set
from src.validation import ValidationError, validate_claims

from tests.test_validation import make_valid_claim


def rule_set(*rules: dict) -> RuleSet:
    return RuleSet.model_validate({"version": 1, "rules": list(rules)})


def frame(*records: dict) -> pd.DataFrame:
    return pd.DataFrame(list(records))


def write_rules(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


# Schema checks: bad config fails at load time, not mid-batch

def test_shipped_rules_file_is_valid(settings):
    rules = load_rule_set(settings.validation_rules_file)
    assert len(rules.active_rules) >= 15


def test_unknown_rule_type_is_rejected(tmp_path):
    path = write_rules(tmp_path, {"rules": [{"code": "X", "type": "regex", "column": "a"}]})
    with pytest.raises(RuleConfigError):
        load_rule_set(path)


def test_misspelled_key_is_rejected(tmp_path):
    path = write_rules(tmp_path, {"rules": [{"code": "X", "type": "required", "colums": ["a"]}]})
    with pytest.raises(RuleConfigError, match="colums"):
        load_rule_set(path)


def test_compare_needs_exactly_one_right_side(tmp_path):
    path = write_rules(
        tmp_path,
        {"rules": [{"code": "X", "type": "compare", "as": "number", "left": "a", "op": ">"}]},
    )
    with pytest.raises(RuleConfigError, match="right_column or right_value"):
        load_rule_set(path)


def test_lowercase_code_is_rejected():
    with pytest.raises(Exception):
        rule_set({"code": "bad_code", "type": "required", "columns": ["a"]})


def test_missing_rules_file_is_reported(tmp_path):
    with pytest.raises(RuleConfigError, match="not found"):
        load_rule_set(tmp_path / "nope.yaml")


# Rule behavior

def test_required_treats_blank_as_missing(settings):
    rules = rule_set({"code": "MISSING", "type": "required", "columns": ["a"]})
    mask = evaluate_rule(frame({"a": "x"}, {"a": "  "}, {"a": None}), rules.rules[0], settings)
    assert mask.tolist() == [False, True, True]


def test_when_condition_limits_the_rule(settings):
    rules = rule_set(
        {"code": "NEED_REASON", "type": "required", "columns": ["reason"],
         "when": {"column": "status", "in": ["Denied"]}}
    )
    data = frame({"status": "Denied", "reason": None}, {"status": "Paid", "reason": None})
    assert evaluate_rule(data, rules.rules[0], settings).tolist() == [True, False]


def test_numeric_rule_rejects_unparseable_even_when_blank_is_allowed(settings):
    rules = rule_set({"code": "BAD", "type": "numeric", "column": "amt", "min": 0, "allow_missing": True})
    data = frame({"amt": "10"}, {"amt": None}, {"amt": "abc"}, {"amt": "-1"})
    assert evaluate_rule(data, rules.rules[0], settings).tolist() == [False, False, True, True]


def test_numeric_exclusive_minimum(settings):
    rules = rule_set(
        {"code": "BAD", "type": "numeric", "column": "amt", "min": 0,
         "min_inclusive": False, "allow_missing": False}
    )
    data = frame({"amt": "0"}, {"amt": "0.01"}, {"amt": None})
    assert evaluate_rule(data, rules.rules[0], settings).tolist() == [True, False, True]


def test_compare_skips_rows_with_a_missing_side(settings):
    rules = rule_set(
        {"code": "GT", "type": "compare", "as": "number", "left": "paid", "op": ">", "right_column": "approved"}
    )
    data = frame({"paid": "5", "approved": "4"}, {"paid": "5", "approved": None}, {"paid": "x", "approved": "1"})
    assert evaluate_rule(data, rules.rules[0], settings).tolist() == [True, False, False]


def test_allowed_values_from_settings(settings):
    rules = rule_set(
        {"code": "BAD_STATUS", "type": "allowed_values", "column": "s", "values_from": "schema.allowed_statuses"}
    )
    data = frame({"s": "Paid"}, {"s": "Lost"}, {"s": None})
    assert evaluate_rule(data, rules.rules[0], settings).tolist() == [False, True, True]


def test_values_from_unknown_path_fails(settings):
    rules = rule_set({"code": "X", "type": "allowed_values", "column": "s", "values_from": "schema.nope"})
    with pytest.raises(RuleConfigError):
        evaluate_rule(frame({"s": "a"}), rules.rules[0], settings)


# Rules inside validate_claims

def test_multiple_reasons_follow_rule_order():
    record = make_valid_claim()
    record["billed_amount"] = -5
    record["claim_status"] = "Lost"

    result = validate_claims(pd.DataFrame([record]))

    assert result.rejected_records.iloc[0]["rejection_reason"] == (
        "INVALID_BILLED_AMOUNT|APPROVED_EXCEEDS_BILLED|INVALID_CLAIM_STATUS"
    )


def test_warn_rule_keeps_the_row(settings):
    record = make_valid_claim()
    record["denial_reason"] = "Duplicate claim"  # on an Approved claim

    result = validate_claims(pd.DataFrame([record]), settings)

    assert len(result.valid_records) == 1
    assert result.summary["warning_hits"]["DENIAL_REASON_ON_APPROVED_CLAIM"] == 1


def test_disabled_rule_does_not_fire(settings):
    rules = rule_set(
        {"code": "INVALID_BILLED_AMOUNT", "type": "numeric", "column": "billed_amount",
         "min": 0, "min_inclusive": False, "enabled": False}
    )
    record = make_valid_claim()
    record["billed_amount"] = -5

    result = validate_claims(pd.DataFrame([record]), settings, rule_set=rules)

    assert len(result.rejected_records) == 0


def test_shared_code_appears_once():
    rules = rule_set(
        {"code": "MISSING_AMOUNT", "type": "required", "columns": ["approved_amount"]},
        {"code": "MISSING_AMOUNT", "type": "required", "columns": ["approved_amount", "paid_amount"]},
    )
    record = make_valid_claim()
    record["approved_amount"] = None

    result = validate_claims(pd.DataFrame([record]), rule_set=rules)

    assert result.rejected_records.iloc[0]["rejection_reason"] == "MISSING_AMOUNT"


def test_rule_on_missing_column_fails_fast():
    rules = rule_set({"code": "X", "type": "required", "columns": ["no_such_column"]})

    with pytest.raises(ValidationError, match="no_such_column"):
        validate_claims(pd.DataFrame([make_valid_claim()]), rule_set=rules)


def test_rule_hits_are_counted():
    good, bad = make_valid_claim(), make_valid_claim()
    bad["claim_id"] = "CLM002"
    bad["claim_status"] = "Denied"
    bad["paid_amount"] = 0

    result = validate_claims(pd.DataFrame([good, bad]))

    assert result.summary["rule_hits"]["MISSING_DENIAL_REASON"] == 1
    assert result.summary["by_file"]["test.csv"] == {"received": 2, "rejected": 1}
