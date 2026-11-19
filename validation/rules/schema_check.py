"""Rule 3: required-column, data-type, and allowed-value conformance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from pydantic import ValidationError

from etl.contracts import BatchViolation, RuleFamily, TransactionRecord, Violation


@dataclass(frozen=True)
class SchemaCheckResult:
    """Outcome of the schema family, including successfully parsed records."""

    batch_violations: list[BatchViolation] = field(default_factory=list)
    row_violations: list[Violation] = field(default_factory=list)
    parsed_rows: dict[int, TransactionRecord] = field(default_factory=dict)


def _is_blank(value: Any) -> bool:
    return value is None or not str(value).strip()


def check_schema(
    frame: pd.DataFrame,
    required_columns: list[str],
    allowed_values: dict[str, list[str]] | None = None,
) -> SchemaCheckResult:
    """Validate the batch header, then every row against ``TransactionRecord``.

    Missing required columns are a blocking batch violation: no row can be
    trusted and nothing should reach the target table.  Per-row type and enum
    failures are returned as quarantinable row violations alongside parsed
    records for the clean rows.
    """

    allowed_values = allowed_values or {}
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        message = "Missing required column(s): " + ", ".join(missing_columns)
        return SchemaCheckResult(
            batch_violations=[BatchViolation(family=RuleFamily.SCHEMA, message=message)]
        )

    row_violations: list[Violation] = []
    parsed_rows: dict[int, TransactionRecord] = {}

    for position in range(len(frame)):
        row = frame.iloc[position]
        row_number = position + 2
        payload = {column: row[column] for column in required_columns}
        try:
            parsed_rows[row_number] = TransactionRecord.model_validate(payload)
        except ValidationError as exc:
            for error in exc.errors():
                location = error.get("loc", ())
                field_name = str(location[0]) if location else None
                reason = str(error.get("msg", "invalid value"))
                row_violations.append(
                    Violation(
                        family=RuleFamily.SCHEMA,
                        field=field_name,
                        row_number=row_number,
                        message=f"Invalid value for '{field_name}': {reason}",
                    )
                )

    for field_name, allowed in allowed_values.items():
        if field_name not in frame.columns:
            continue
        allowed_set = set(allowed)
        for position, value in enumerate(frame[field_name].tolist()):
            if _is_blank(value):
                continue
            normalized = str(value).strip()
            if normalized not in allowed_set:
                row_violations.append(
                    Violation(
                        family=RuleFamily.SCHEMA,
                        field=field_name,
                        row_number=position + 2,
                        message=(
                            f"Value '{normalized}' for '{field_name}' is not allowed; "
                            f"expected one of {sorted(allowed_set)}"
                        ),
                    )
                )

    row_violations.sort(key=lambda violation: violation.row_number or 0)
    return SchemaCheckResult(row_violations=row_violations, parsed_rows=parsed_rows)
