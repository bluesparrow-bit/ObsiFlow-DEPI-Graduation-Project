"""Focused regression tests for the null, duplicate, and schema rule families.

These tests exercise the core rule behavior and the hand-off between the null
and schema rules.  They use the checked-in ``dq_config`` fixture so drift in the
shared rule catalogue is caught here instead of in production.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd
import pytest

from config.settings import DQConfig
from etl.contracts import RuleFamily, TransactionRecord
from validation.rules import check_schema, find_duplicates, find_null_violations


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


def make_frame(*rows: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("account_id", ""),
        ("amount", "   "),
        ("txn_date", None),
        ("transaction_id", float("nan")),
    ],
    ids=["empty-string", "whitespace", "none", "nan"],
)
def test_null_rule_flags_blank_mandatory_values(field_name: str, value: object) -> None:
    violations = find_null_violations(make_frame(base_row(**{field_name: value})), [field_name])

    assert len(violations) == 1
    assert violations[0].family is RuleFamily.NULL
    assert violations[0].field == field_name
    assert violations[0].row_number == 2


@pytest.mark.unit
def test_null_rule_ignores_missing_columns_and_sorts_by_row() -> None:
    records = pd.DataFrame(
        [
            base_row(account_id="", amount=""),
            base_row(amount="  "),
        ]
    )
    violations = find_null_violations(records, ["account_id", "amount", "not_a_column"])

    assert [(violation.row_number, violation.field) for violation in violations] == [
        (2, "account_id"),
        (2, "amount"),
        (3, "amount"),
    ]


@pytest.mark.unit
def test_null_rule_accepts_clean_rows() -> None:
    assert find_null_violations(make_frame(base_row()), ["transaction_id", "amount"]) == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("keys", "policy", "expected_rows"),
    [
        (["TXN1", "TXN2"], "keep_first", []),
        (["TXN1", "TXN1"], "keep_first", [3]),
        (["TXN1", "TXN1"], "keep_last", [2]),
        (["TXN1", "txn1"], "keep_first", [3]),
        ([" TXN1 ", "txn1"], "keep_last", [2]),
        (["", "   "], "keep_first", []),
    ],
    ids=[
        "unique",
        "keep-first",
        "keep-last",
        "case-insensitive",
        "whitespace-and-case",
        "blank-ignored",
    ],
)
def test_duplicate_rule_policies(keys: list[str], policy: str, expected_rows: list[int]) -> None:
    records = make_frame(*(base_row(transaction_id=key) for key in keys))
    violations = find_duplicates(
        records,
        "transaction_id",
        policy=policy,  # type: ignore[arg-type]
    )

    assert [violation.row_number for violation in violations] == expected_rows
    assert all(violation.family is RuleFamily.DUPLICATE for violation in violations)
    assert all(violation.field == "transaction_id" for violation in violations)


@pytest.mark.unit
def test_duplicate_rule_reports_each_rejected_row_once() -> None:
    records = make_frame(*(base_row(transaction_id="TXN1") for _ in range(3)))

    violations = find_duplicates(records, "transaction_id", policy="keep_first")

    assert [violation.row_number for violation in violations] == [3, 4]


@pytest.mark.unit
def test_duplicate_rule_ignores_missing_key_column() -> None:
    records = make_frame(base_row()).drop(columns=["transaction_id"])

    assert find_duplicates(records, "transaction_id") == []


@pytest.mark.unit
def test_duplicate_rule_rejects_unknown_policy() -> None:
    with pytest.raises(ValueError, match="Unsupported duplicate policy"):
        find_duplicates(
            make_frame(base_row()),
            "transaction_id",
            policy="keep_all",  # type: ignore[arg-type]
        )


@pytest.mark.unit
def test_schema_missing_required_column_blocks_the_batch(dq_config: DQConfig) -> None:
    result = check_schema(
        make_frame(base_row()).drop(columns=["amount"]),
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )

    assert len(result.batch_violations) == 1
    assert result.batch_violations[0].family is RuleFamily.SCHEMA
    assert result.batch_violations[0].blocking is True
    assert result.row_violations == []
    assert result.parsed_rows == {}


@pytest.mark.unit
def test_schema_parses_clean_rows_and_reports_type_and_enum_failures(
    dq_config: DQConfig,
) -> None:
    result = check_schema(
        make_frame(
            base_row(),
            base_row(amount="abc"),
            base_row(currency="usd"),
            base_row(txn_type="REFUND"),
            base_row(currency="usd", txn_type="REFUND"),
        ),
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )

    assert result.batch_violations == []
    assert list(result.parsed_rows) == [2]
    assert isinstance(result.parsed_rows[2], TransactionRecord)
    assert Counter(
        (violation.row_number, violation.field) for violation in result.row_violations
    ) == Counter(
        [
            (3, "amount"),
            (4, "currency"),
            (5, "txn_type"),
            (6, "currency"),
            (6, "txn_type"),
        ]
    )
    assert all(violation.family is RuleFamily.SCHEMA for violation in result.row_violations)


@pytest.mark.unit
def test_schema_reordered_and_extra_columns_are_non_blocking(dq_config: DQConfig) -> None:
    required_columns = list(dq_config.schema_rules.required_columns)
    records = make_frame(base_row())[list(reversed(required_columns))].copy()
    records["source_system"] = "legacy"

    result = check_schema(
        records,
        required_columns=required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )

    assert [violation.blocking for violation in result.batch_violations] == [False, False]
    assert all(violation.family is RuleFamily.SCHEMA for violation in result.batch_violations)
    assert any("Column order" in violation.message for violation in result.batch_violations)
    assert any(
        "Unexpected extra column" in violation.message for violation in result.batch_violations
    )
    assert list(result.parsed_rows) == [2]


@pytest.mark.unit
def test_schema_strips_header_and_cell_whitespace(dq_config: DQConfig) -> None:
    required_columns = list(dq_config.schema_rules.required_columns)
    records = make_frame(base_row(transaction_id="  TXN00000001  ")).rename(
        columns={column: f" {column} " for column in required_columns}
    )

    result = check_schema(
        records,
        required_columns=required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )

    assert result.batch_violations == []
    assert result.row_violations == []
    assert result.parsed_rows[2].transaction_id == "TXN00000001"


@pytest.mark.unit
def test_schema_skips_blank_mandatory_errors_for_the_null_rule(dq_config: DQConfig) -> None:
    records = make_frame(base_row(amount=""))

    result = check_schema(
        records,
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
        mandatory_fields=dq_config.fields.mandatory,
    )

    assert result.row_violations == []
    assert result.parsed_rows == {}

    null_violations = find_null_violations(records, dq_config.fields.mandatory)
    assert len(null_violations) == 1
    assert null_violations[0].family is RuleFamily.NULL
    assert null_violations[0].field == "amount"


@pytest.mark.unit
def test_schema_keeps_non_blank_mandatory_type_errors(dq_config: DQConfig) -> None:
    result = check_schema(
        make_frame(base_row(amount="abc")),
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
        mandatory_fields=dq_config.fields.mandatory,
    )

    assert [(violation.row_number, violation.field) for violation in result.row_violations] == [
        (2, "amount")
    ]
    assert result.row_violations[0].family is RuleFamily.SCHEMA
    assert result.parsed_rows == {}


@pytest.mark.unit
def test_schema_default_reports_blank_mandatory_as_schema_error(dq_config: DQConfig) -> None:
    result = check_schema(
        make_frame(base_row(amount="")),
        required_columns=dq_config.schema_rules.required_columns,
        allowed_values=dq_config.schema_rules.allowed_values,
    )

    assert [(violation.row_number, violation.field) for violation in result.row_violations] == [
        (2, "amount")
    ]
    assert result.parsed_rows == {}
