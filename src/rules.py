"""
Configuration-driven validation rules.

Rules live in config/validation_rules.yaml. This module

1. parses that file into typed rule objects, rejecting unknown keys, unknown
   rule types and unknown operators before any data is read, and
2. evaluates each rule against a DataFrame as a single vectorized boolean
   mask ("True" means the row breaks the rule).

Keeping the rule definitions in data rather than code means an analyst can
add a domain value, switch a rule to warn-only, or tighten a threshold with a
reviewed config change instead of a code change. The set of rule *types* is
deliberately small and closed: anything a rule needs that the types below
cannot express belongs in code, with a test.
"""

from __future__ import annotations

import operator
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Union

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

if TYPE_CHECKING:
    from .config import Settings


class RuleConfigError(Exception):
    """Raised when the rules file is missing or does not match the schema."""


Severity = Literal["reject", "warn"]

_OPERATORS: dict[str, Callable[[Any, Any], Any]] = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}


# Rule schema

class Condition(BaseModel):
    """Apply a rule only where `column` takes one of the listed values."""

    model_config = ConfigDict(extra="forbid")

    column: str
    in_: list[str] = Field(alias="in", min_length=1)


class _BaseRule(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    severity: Severity = "reject"
    enabled: bool = True
    description: str = ""
    when: Condition | None = None

    def referenced_columns(self) -> set[str]:
        columns = self._own_columns()
        if self.when is not None:
            columns.add(self.when.column)
        return columns

    def _own_columns(self) -> set[str]:
        raise NotImplementedError


class RequiredRule(_BaseRule):
    """Fail when any listed column is NULL or blank."""

    type: Literal["required"]
    # Empty means "use schema.required_fields from settings.yaml".
    columns: list[str] = Field(default_factory=list)

    def _own_columns(self) -> set[str]:
        return set(self.columns)


class MustBeEmptyRule(_BaseRule):
    """Fail when any listed column has a non-blank value."""

    type: Literal["must_be_empty"]
    columns: list[str] = Field(min_length=1)

    def _own_columns(self) -> set[str]:
        return set(self.columns)


class DateParseableRule(_BaseRule):
    """Fail when the column has a value that cannot be parsed as a date."""

    type: Literal["date_parseable"]
    column: str

    def _own_columns(self) -> set[str]:
        return {self.column}


class NotFutureDateRule(_BaseRule):
    """Fail when the parsed date is later than today (UTC)."""

    type: Literal["not_future_date"]
    column: str

    def _own_columns(self) -> set[str]:
        return {self.column}


class NumericRule(_BaseRule):
    """
    Fail when a value is not a number, or falls outside [min, max].

    A non-blank value that cannot be parsed always fails: "abc" is never a
    valid amount, even in a column where a blank is acceptable.
    """

    type: Literal["numeric"]
    column: str
    min: float | None = None
    max: float | None = None
    min_inclusive: bool = True
    max_inclusive: bool = True
    allow_missing: bool = True

    def _own_columns(self) -> set[str]:
        return {self.column}


class CompareRule(_BaseRule):
    """
    Fail when `left <op> right` is true.

    Rows where either side is missing or unparseable are skipped; those are
    the job of the required, date_parseable and numeric rules.
    """

    type: Literal["compare"]
    as_: Literal["number", "date"] = Field(alias="as")
    left: str
    op: Literal["<", "<=", ">", ">=", "==", "!="]
    right_column: str | None = None
    right_value: float | str | None = None

    @model_validator(mode="after")
    def _exactly_one_right_side(self) -> CompareRule:
        if (self.right_column is None) == (self.right_value is None):
            raise ValueError(
                "compare rules need exactly one of right_column or right_value"
            )
        return self

    def _own_columns(self) -> set[str]:
        columns = {self.left}
        if self.right_column:
            columns.add(self.right_column)
        return columns


class AllowedValuesRule(_BaseRule):
    """Fail when the value is not in the allowed list."""

    type: Literal["allowed_values"]
    column: str
    values: list[str] | None = None
    # Dotted path into settings.yaml, e.g. "schema.allowed_statuses".
    values_from: str | None = None
    allow_missing: bool = False

    @model_validator(mode="after")
    def _exactly_one_source(self) -> AllowedValuesRule:
        if (self.values is None) == (self.values_from is None):
            raise ValueError(
                "allowed_values rules need exactly one of values or values_from"
            )
        return self

    def _own_columns(self) -> set[str]:
        return {self.column}


Rule = Annotated[
    Union[
        RequiredRule,
        MustBeEmptyRule,
        DateParseableRule,
        NotFutureDateRule,
        NumericRule,
        CompareRule,
        AllowedValuesRule,
    ],
    Field(discriminator="type"),
]


class RuleSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    rules: list[Rule] = Field(min_length=1)

    @property
    def active_rules(self) -> list[Rule]:
        return [rule for rule in self.rules if rule.enabled]

    def referenced_columns(self, settings: Settings) -> set[str]:
        columns: set[str] = set()
        for rule in self.active_rules:
            columns |= rule.referenced_columns()
            if isinstance(rule, RequiredRule) and not rule.columns:
                columns |= set(settings.required_fields)
        return columns


# Loading

def load_rule_set(path: Path) -> RuleSet:
    """Read and validate a rules file. Raises RuleConfigError on any problem."""

    if not path.exists():
        raise RuleConfigError(f"Validation rules file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    try:
        return RuleSet.model_validate(raw)
    except ValidationError as exc:
        raise RuleConfigError(f"Invalid rules in {path.name}:\n{exc}") from exc


@lru_cache(maxsize=8)
def _cached_rule_set(path: str, mtime_ns: int) -> RuleSet:
    return load_rule_set(Path(path))


def get_rule_set(path: Path) -> RuleSet:
    """
    Cached load, keyed on the file's modification time, so a long-lived
    process (the API, Airflow worker) picks up an edited rules file without a
    restart and an unchanged file is parsed once.
    """

    if not path.exists():
        raise RuleConfigError(f"Validation rules file not found: {path}")

    return _cached_rule_set(str(path), path.stat().st_mtime_ns)


def resolve_values_from(dotted: str, settings: Settings) -> list[str]:
    """Look up a list such as "schema.allowed_statuses" in settings.yaml."""

    node: Any = settings.raw_config
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            raise RuleConfigError(f"values_from path not found in settings: {dotted}")
        node = node[part]

    if not isinstance(node, list):
        raise RuleConfigError(f"values_from must point at a list: {dotted}")

    return [str(value) for value in node]


# Evaluation helpers

def _is_blank(series: pd.Series) -> pd.Series:
    """NULL, or a string that is empty after trimming."""

    return series.isna() | series.astype("string").str.strip().eq("").fillna(False).astype(bool)


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _condition_mask(dataframe: pd.DataFrame, rule: _BaseRule) -> pd.Series:
    if rule.when is None:
        return pd.Series(True, index=dataframe.index)
    return dataframe[rule.when.column].isin(rule.when.in_)


def evaluate_rule(
    dataframe: pd.DataFrame,
    rule: Rule,
    settings: Settings,
) -> pd.Series:
    """Return a boolean Series that is True where the row breaks the rule."""

    applies = _condition_mask(dataframe, rule)

    if isinstance(rule, RequiredRule):
        columns = rule.columns or settings.required_fields
        failed = pd.Series(False, index=dataframe.index)
        for column in columns:
            failed |= _is_blank(dataframe[column])

    elif isinstance(rule, MustBeEmptyRule):
        failed = pd.Series(False, index=dataframe.index)
        for column in rule.columns:
            failed |= ~_is_blank(dataframe[column])

    elif isinstance(rule, DateParseableRule):
        raw = dataframe[rule.column]
        failed = raw.notna() & _to_date(raw).isna()

    elif isinstance(rule, NotFutureDateRule):
        parsed = _to_date(dataframe[rule.column])
        today = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
        failed = parsed.notna() & (parsed.dt.normalize() > today)

    elif isinstance(rule, NumericRule):
        raw = dataframe[rule.column]
        number = _to_number(raw)
        blank = _is_blank(raw)
        unparseable = ~blank & number.isna()
        failed = unparseable | (blank if not rule.allow_missing else False)
        if rule.min is not None:
            below = number < rule.min if rule.min_inclusive else number <= rule.min
            failed |= number.notna() & below
        if rule.max is not None:
            above = number > rule.max if rule.max_inclusive else number >= rule.max
            failed |= number.notna() & above

    elif isinstance(rule, CompareRule):
        convert = _to_date if rule.as_ == "date" else _to_number
        left = convert(dataframe[rule.left])
        if rule.right_column is not None:
            right = convert(dataframe[rule.right_column])
            both_present = left.notna() & right.notna()
        else:
            right = (
                pd.Timestamp(rule.right_value)
                if rule.as_ == "date"
                else float(rule.right_value)
            )
            both_present = left.notna()
        compared = _OPERATORS[rule.op](left, right)
        failed = both_present & compared.fillna(False).astype(bool)

    elif isinstance(rule, AllowedValuesRule):
        allowed = (
            rule.values
            if rule.values is not None
            else resolve_values_from(rule.values_from, settings)
        )
        raw = dataframe[rule.column]
        failed = ~raw.isin(allowed)
        if rule.allow_missing:
            failed &= raw.notna()

    else:  # pragma: no cover - the discriminated union makes this unreachable
        raise RuleConfigError(f"Unsupported rule type: {type(rule).__name__}")

    return (applies & failed).fillna(False).astype(bool)


def describe_rules(rule_set: RuleSet) -> list[dict[str, Any]]:
    """Flat description of every rule, for the quality summary and dashboard."""

    return [
        {
            "code": rule.code,
            "type": rule.type,
            "severity": rule.severity,
            "enabled": rule.enabled,
            "description": rule.description,
        }
        for rule in rule_set.rules
    ]
