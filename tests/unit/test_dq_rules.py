"""Parametrized pass/fail tests for all six DQ rule families and the engine."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import pytest
from pydantic import ValidationError

from config.settings import DQConfig
from etl.contracts import RuleFamily, TransactionRecord
from validation.engine import validate_batch
from validation.rules import (
    check_ranges,
    check_referential,
    check_row_count,
    check_schema,
    find_duplicates,
    find_null_violations,
)

MANDATORY = ["transaction_id", "account_id", "txn_type", "amount", "txn_date"]

SKIP_PENDING = pytest.mark.skip(reason="Pending role implementation")


def make_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def base_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "transaction_id": "TXN00000001",
        "account_id": "ACC000001",
        "txn_type": "DEPOSIT",
        "amount": "100.00",
        "currency": "USD",
        "txn_date": "2025-01-15",
        "channel": "ONLINE",
        "status": "POSTED",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Rule 1: null checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "expected_count"),
    [
        ({}, 0),
        ({"account_id": ""}, 1),
        ({"amount": "   "}, 1),
        ({"txn_date": None}, 1),
    ],
    ids=["clean", "empty-account", "whitespace-amount", "null-date"],
)
@SKIP_PENDING
def test_null_check_pass_fail(overrides: dict[str, Any], expected_count: int) -> None:
    violations = find_null_violations(make_frame([base_row(**overrides)]), MANDATORY)
    assert len(violations) == expected_count
    if violations:
        assert violations[0].family is RuleFamily.NULL
        assert violations[0].row_number == 2


# ---------------------------------------------------------------------------
# Rule 2: duplicate checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("keys", "policy", "expected_rows"),
    [
        (["TXN1", "TXN2", "TXN3"], "keep_first", []),
        (["TXN1", "TXN1", "TXN2"], "keep_first", [3]),
        (["TXN1", "TXN1", "TXN2"], "keep_last", [2]),
        (["TXN1", "txn1", "TXN2"], "keep_first", [3]),
    ],
    ids=["unique", "duplicate-later", "keep-last", "case-insensitive"],
)
@SKIP_PENDING
def test_duplicate_check_pass_fail(keys: list[str], policy: str, expected_rows: list[int]) -> None:
    frame = make_frame([base_row(transaction_id=key) for key in keys])
    violations = find_duplicates(frame, "transaction_id", policy=policy)  # type: ignore[arg-type]
    assert [violation.row_number for violation in violations] == expected_rows
    assert all(violation.family is RuleFamily.DUPLICATE for violation in violations)


# ---------------------------------------------------------------------------
# Rule 3: schema checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "expected_count"),
    [
        ({}, 0),
        ({"amount": "abc"}, 1),
        ({"txn_date": "31/13/2025"}, 1),
        ({"currency": "usd"}, 1),
        ({"txn_type": "REFUND"}, 1),
    ],
    ids=["clean", "non-numeric-amount", "malformed-date", "lowercase-currency", "bad-enum"],
)
@SKIP_PENDING
def test_schema_check_pass_fail(
    overrides: dict[str, Any],
    expected_count: int,
    dq_config: DQConfig,
) -> None:
    result = check_schema(
        make_frame([base_row(**overrides)]),
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )
    assert not result.batch_violations
    assert len(result.row_violations) == expected_count
    assert all(violation.family is RuleFamily.SCHEMA for violation in result.row_violations)


@pytest.mark.unit
@SKIP_PENDING
def test_schema_missing_column_is_blocking(dq_config: DQConfig) -> None:
    frame = make_frame([base_row()]).drop(columns=["amount"])
    result = check_schema(
        frame,
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )
    assert len(result.batch_violations) == 1
    assert result.batch_violations[0].family is RuleFamily.SCHEMA
    assert result.batch_violations[0].blocking
    assert not result.parsed_rows


# ---------------------------------------------------------------------------
# Rule 4: row-count checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("total", "expected_count"),
    [(10, 0), (100, 0), (9, 1), (101, 1)],
    ids=["min-boundary", "inside", "too-few", "too-many"],
)
@SKIP_PENDING
def test_row_count_check_pass_fail(total: int, expected_count: int) -> None:
    violations = check_row_count(total_rows=total, min_rows=10, max_rows=100)
    assert len(violations) == expected_count
    assert all(violation.family is RuleFamily.ROW_COUNT for violation in violations)


# ---------------------------------------------------------------------------
# Rule 5: range checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "expected_count"),
    [
        ({}, 0),
        ({"amount": "0.01"}, 0),
        ({"amount": "1000000.00"}, 0),
        ({"amount": "0.00"}, 1),
        ({"amount": "-1.00"}, 1),
        ({"amount": "1000000.01"}, 1),
        ({"txn_date": "2019-12-31"}, 1),
        ({"txn_date": "2026-11-06"}, 1),
    ],
    ids=[
        "clean",
        "amount-min",
        "amount-max",
        "amount-zero",
        "amount-negative",
        "amount-too-high",
        "date-too-old",
        "date-too-new",
    ],
)
@SKIP_PENDING
def test_range_check_pass_fail(overrides: dict[str, Any], expected_count: int) -> None:
    violations = check_ranges(
        make_frame([base_row(**overrides)]),
        amount_min=0.01,
        amount_max=1_000_000.00,
        date_min=date(2020, 1, 1),
        date_max=date(2026, 11, 5),
    )
    assert len(violations) == expected_count
    assert all(violation.family is RuleFamily.RANGE for violation in violations)


# ---------------------------------------------------------------------------
# Rule 6: referential checks
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    ("account_id", "expected_count"),
    [("ACC000001", 0), ("acc000001", 0), ("ACC999999", 1), ("", 0)],
    ids=["known", "case-insensitive", "orphan", "blank-ignored"],
)
@SKIP_PENDING
def test_referential_check_pass_fail(account_id: str, expected_count: int) -> None:
    violations = check_referential(
        make_frame([base_row(account_id=account_id)]),
        "account_id",
        {"ACC000001", "ACC000002"},
    )
    assert len(violations) == expected_count
    assert all(violation.family is RuleFamily.REFERENTIAL for violation in violations)


# ---------------------------------------------------------------------------
# Engine integration of all six families
# ---------------------------------------------------------------------------
@pytest.mark.unit
@SKIP_PENDING
def test_engine_clean_sample_has_no_rejections(
    sample_valid_frame: pd.DataFrame,
    dq_config: DQConfig,
    reference_ids: set[str],
) -> None:
    result = validate_batch(sample_valid_frame, dq_config, reference_ids, "sample_valid.csv")
    assert result.total_rows == 20
    assert len(result.valid_records) == 20
    assert result.rejected_rows == []
    assert result.is_blocking is False
    assert result.rejection_rate == 0.0
    assert all(count == 0 for count in result.failures_by_family.values())


@pytest.mark.unit
@SKIP_PENDING
def test_engine_corrupted_sample_exercises_all_six_families(
    sample_corrupted_frame: pd.DataFrame,
    dq_config: DQConfig,
    reference_ids: set[str],
) -> None:
    result = validate_batch(
        sample_corrupted_frame, dq_config, reference_ids, "sample_corrupted.csv"
    )
    assert result.total_rows == 9
    assert len(result.rejected_rows) == 8
    assert len(result.valid_records) == 1
    assert result.is_blocking is True
    assert result.rejection_rate == pytest.approx(8 / 9, rel=1e-5)

    for family in RuleFamily:
        assert result.failures_by_family[family.value] > 0, family

    blocking_families = {violation.family for violation in result.batch_violations}
    assert blocking_families == {RuleFamily.ROW_COUNT}
    assert result.failures_by_family[RuleFamily.ROW_COUNT.value] == 1


@pytest.mark.unit
@SKIP_PENDING
def test_engine_missing_column_rejects_entire_batch(
    sample_valid_frame: pd.DataFrame,
    dq_config: DQConfig,
    reference_ids: set[str],
) -> None:
    broken = sample_valid_frame.drop(columns=["amount"])
    result = validate_batch(broken, dq_config, reference_ids, "missing-amount.csv")
    assert result.valid_records == []
    assert len(result.rejected_rows) == 20
    assert result.is_blocking is True
    assert result.failures_by_family[RuleFamily.SCHEMA.value] == 21  # 20 rows + 1 batch


@pytest.mark.unit
@SKIP_PENDING
def test_engine_rejection_threshold_is_alert_only(
    dq_config: DQConfig,
    reference_ids: set[str],
) -> None:
    rows = [base_row(transaction_id=f"TXN{i:03d}") for i in range(10)]
    rows.append(base_row(transaction_id="TXN100", amount="-5.00"))
    rows.append(base_row(transaction_id="TXN999", account_id="ACC999999"))
    result = validate_batch(
        make_frame(rows),
        dq_config,
        reference_ids,
        "mixed.csv",
    )
    assert len(result.valid_records) == 10
    assert len(result.rejected_rows) == 2
    assert result.rejection_threshold_exceeded is True
    assert result.is_blocking is False


# ---------------------------------------------------------------------------
# Contract strictness (schema rule depends on this)
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        {**base_row(), "amount": "1,000.00"},
        {**base_row(), "amount": "NaN"},
        {**base_row(), "amount": "$100"},
        {**base_row(), "txn_date": "2025/01/15"},
        {**base_row(), "txn_date": "2025-02-30"},
        {**base_row(), "extra_column": "x"},
    ],
    ids=[
        "comma-amount",
        "nan-amount",
        "currency-symbol",
        "slash-date",
        "invalid-date",
        "extra-field",
    ],
)
def test_transaction_contract_rejects_corrupt_values(payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        TransactionRecord.model_validate(payload)
