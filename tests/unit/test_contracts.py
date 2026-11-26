"""Unit tests for shared data contracts and RunCounters."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from etl.contracts import (
    PipelineRun,
    RejectedRow,
    RuleFamily,
    RunCounters,
    RunStatus,
    TransactionRecord,
    Violation,
)


@pytest.mark.unit
def test_transaction_record_valid() -> None:
    record = TransactionRecord(
        transaction_id="TX100",
        account_id="ACC001",
        txn_type="DEPOSIT",
        amount=123.45,
        currency="USD",
        txn_date="2025-05-01",
        channel="WEB",
        status="COMPLETED",
    )
    assert record.transaction_id == "TX100"
    assert record.amount == Decimal("123.45")
    assert record.txn_date == date(2025, 5, 1)


@pytest.mark.unit
def test_transaction_record_rejects_blank_strings() -> None:
    with pytest.raises(ValidationError):
        TransactionRecord(
            transaction_id="   ",
            account_id="ACC001",
            txn_type="DEPOSIT",
            amount=100,
            currency="USD",
            txn_date="2025-05-01",
            channel="WEB",
            status="COMPLETED",
        )


@pytest.mark.unit
def test_transaction_record_amount_validation() -> None:
    # Boolean amount rejected
    with pytest.raises(ValidationError, match="amount must be numeric"):
        TransactionRecord(
            transaction_id="TX1",
            account_id="ACC001",
            txn_type="DEPOSIT",
            amount=True,
            currency="USD",
            txn_date="2025-05-01",
            channel="WEB",
            status="COMPLETED",
        )

    # Commas or currency symbols rejected
    with pytest.raises(ValidationError, match="amount must be a plain decimal number"):
        TransactionRecord(
            transaction_id="TX1",
            account_id="ACC001",
            txn_type="DEPOSIT",
            amount="$1,000.00",
            currency="USD",
            txn_date="2025-05-01",
            channel="WEB",
            status="COMPLETED",
        )


@pytest.mark.unit
def test_transaction_record_date_validation() -> None:
    # Datetime with time component rejected
    with pytest.raises(ValidationError, match="txn_date must be a date, not a datetime"):
        TransactionRecord(
            transaction_id="TX1",
            account_id="ACC001",
            txn_type="DEPOSIT",
            amount=100,
            currency="USD",
            txn_date=datetime(2025, 5, 1, 12, 0, 0),
            channel="WEB",
            status="COMPLETED",
        )

    # Malformed date string rejected
    with pytest.raises(ValidationError, match="ISO-8601"):
        TransactionRecord(
            transaction_id="TX1",
            account_id="ACC001",
            txn_type="DEPOSIT",
            amount=100,
            currency="USD",
            txn_date="01/05/2025",
            channel="WEB",
            status="COMPLETED",
        )


@pytest.mark.unit
def test_rejected_row_properties() -> None:
    v1 = Violation(
        family=RuleFamily.NULL,
        field="account_id",
        row_number=2,
        message="Missing account",
    )
    v2 = Violation(
        family=RuleFamily.RANGE,
        field="amount",
        row_number=2,
        message="Negative amount",
    )

    rejected = RejectedRow(
        row_number=2,
        transaction_id="TX1",
        raw={"transaction_id": "TX1", "amount": "-10"},
        violations=[v1, v2],
    )

    assert rejected.rule_families == ["null", "range"]
    reasons = rejected.failure_reasons
    assert len(reasons) == 2
    assert reasons[0]["family"] == "null"
    assert reasons[1]["field"] == "amount"


@pytest.mark.unit
def test_run_counters_and_snapshot() -> None:
    counters = RunCounters()
    counters.set_extracted(100)
    counters.add_valid(80)
    counters.add_rejected(20)
    counters.add_loaded(80)

    snap = counters.snapshot()
    assert snap.extracted == 100
    assert snap.valid == 80
    assert snap.rejected == 20
    assert snap.loaded == 80
    assert snap.rejection_rate == 0.2

    counters.reset()
    snap_empty = counters.snapshot()
    assert snap_empty.extracted == 0
    assert snap_empty.rejection_rate == 0.0


@pytest.mark.unit
def test_pipeline_run_model() -> None:
    run = PipelineRun(
        run_id="run_123",
        dag_id="dq_etl_pipeline",
        source_file="batch.csv",
        status=RunStatus.RUNNING,
        started_at=datetime(2025, 1, 1, 0, 0, 0),
    )
    assert run.status == RunStatus.RUNNING
    assert run.records_extracted == 0
