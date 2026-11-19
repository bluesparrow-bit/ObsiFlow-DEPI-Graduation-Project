"""Transformation of validated records into the target-table shape.

Only contract-valid records reach this module.  Transformations are idempotent
and deterministic so a retried Airflow task produces byte-identical rows.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from etl.contracts import PermanentPipelineError, TransactionRecord

_TWO_PLACES = Decimal("0.01")


def transform_record(record: TransactionRecord) -> dict[str, Any]:
    """Normalize one validated record for PostgreSQL loading."""

    amount = record.amount.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
    if amount <= 0:
        raise PermanentPipelineError(
            f"Transaction {record.transaction_id} has a non-positive amount after transformation"
        )

    return {
        "transaction_id": record.transaction_id.strip(),
        "account_id": record.account_id.strip().upper(),
        "txn_type": record.txn_type.strip().upper(),
        "amount": amount,
        "currency": record.currency.strip().upper(),
        "txn_date": record.txn_date,
        "channel": record.channel.strip().upper(),
        "status": record.status.strip().upper(),
    }


def transform_records(records: list[TransactionRecord]) -> list[dict[str, Any]]:
    """Transform a batch of validated records, preserving input order."""

    return [transform_record(record) for record in records]
