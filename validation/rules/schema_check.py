"""Rule 3: required-column, data-type, and allowed-value conformance."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from etl.contracts import BatchViolation, TransactionRecord, Violation


@dataclass(frozen=True)
class SchemaCheckResult:
    """Outcome of the schema family, including successfully parsed records."""

    batch_violations: list[BatchViolation] = field(default_factory=list)
    row_violations: list[Violation] = field(default_factory=list)
    parsed_rows: dict[int, TransactionRecord] = field(default_factory=dict)


def check_schema(
    frame: pd.DataFrame,
    required_columns: list[str],
    allowed_values: dict[str, list[str]] | None = None,
) -> SchemaCheckResult:
    """Validate the batch header, then every row against ``TransactionRecord``.

    Missing required columns are a blocking batch violation: no row can be
    trusted and nothing should reach the target table.  Per-row type and enum
    failures must be returned as quarantinable row violations alongside parsed
    records for the clean rows.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
