"""Transformation interface for validated records.

[DE] Implement deterministic normalization into the target-table shape.  Only
contract-valid records reach this module, and transformations must be idempotent
so a retried Airflow task produces byte-identical rows.
"""

from __future__ import annotations

from typing import Any

from etl.contracts import TransactionRecord


def transform_record(record: TransactionRecord) -> dict[str, Any]:
    """Normalize one validated record for PostgreSQL loading."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def transform_records(records: list[TransactionRecord]) -> list[dict[str, Any]]:
    """Transform a batch of validated records, preserving input order."""

    raise NotImplementedError("TODO [DE]: Implement this function")
