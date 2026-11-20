"""Rule 3: required-column, data-type, and allowed-value conformance."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pandas as pd
from pydantic import ValidationError

from etl.contracts import BatchViolation, RuleFamily, TransactionRecord, Violation

_FIRST_DATA_ROW = 2
_PYDANTIC_VALUE_ERROR_PREFIX = "Value error, "


@dataclass(frozen=True)
class SchemaCheckResult:
    """Outcome of the schema family, including successfully parsed records."""

    batch_violations: list[BatchViolation] = field(default_factory=list)
    row_violations: list[Violation] = field(default_factory=list)
    parsed_rows: dict[int, TransactionRecord] = field(default_factory=dict)


def _cell(value: object) -> str:
    """Normalize a raw cell to a stripped string ('' for null-like values)."""

    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _clean_message(message: str) -> str:
    if message.startswith(_PYDANTIC_VALUE_ERROR_PREFIX):
        return message[len(_PYDANTIC_VALUE_ERROR_PREFIX) :]
    return message


def check_schema(
    frame: pd.DataFrame,
    required_columns: list[str],
    allowed_values: dict[str, list[str]] | None = None,
    mandatory_fields: Sequence[str] = (),
) -> SchemaCheckResult:
    """Validate the batch header, then every row against ``TransactionRecord``.

    Missing required columns are a blocking batch violation: no row can be
    trusted and nothing should reach the target table.  Per-row type and enum
    failures must be returned as quarantinable row violations alongside parsed
    records for the clean rows.

    ``mandatory_fields`` lists the columns owned by the null rule: blank cells
    there are not re-reported here (avoids double counting), but the row is
    still excluded from ``parsed_rows``.
    """

    allowed_values = allowed_values or {}
    mandatory = set(mandatory_fields)

    frame = frame.rename(columns=lambda c: str(c).strip())
    actual_columns = list(frame.columns)

    # ---- 1. Header checks (batch level) -----------------------------------
    missing = [c for c in required_columns if c not in actual_columns]
    if missing:
        return SchemaCheckResult(
            batch_violations=[
                BatchViolation(
                    family=RuleFamily.SCHEMA,
                    message=f"Missing required column(s): {', '.join(missing)}",
                    blocking=True,
                )
            ]
        )

    batch_violations: list[BatchViolation] = []

    present_in_order = [c for c in actual_columns if c in required_columns]
    if present_in_order != list(required_columns):
        batch_violations.append(
            BatchViolation(
                family=RuleFamily.SCHEMA,
                message=(
                    "Column order differs from the contract: expected "
                    f"{list(required_columns)}, got {present_in_order}"
                ),
                blocking=False,
            )
        )

    extra = [c for c in actual_columns if c not in required_columns]
    if extra:
        batch_violations.append(
            BatchViolation(
                family=RuleFamily.SCHEMA,
                message=f"Unexpected extra column(s) ignored: {', '.join(extra)}",
                blocking=False,
            )
        )

    # ---- 2. Row checks -----------------------------------------------------
    row_violations: list[Violation] = []
    parsed_rows: dict[int, TransactionRecord] = {}

    records = frame[list(required_columns)].to_dict("records")

    for position, raw_row in enumerate(records):
        row_number = position + _FIRST_DATA_ROW
        values = {col: _cell(raw_row[col]) for col in required_columns}

        blank_mandatory = {
            col for col in required_columns if col in mandatory and values[col] == ""
        }
        row_problems: list[Violation] = []

        # Type / format conformance via the Pydantic contract.
        record: TransactionRecord | None = None
        try:
            record = TransactionRecord.model_validate(values)
        except ValidationError as exc:
            for err in exc.errors():
                col = str(err["loc"][0]) if err["loc"] else None
                if col in blank_mandatory:
                    continue  # owned by the null rule
                row_problems.append(
                    Violation(
                        family=RuleFamily.SCHEMA,
                        message=f"{col}: {_clean_message(err['msg'])}",
                        field=col,
                        row_number=row_number,
                    )
                )

        # Allowed-value (enum) conformance on non-blank cells.
        for col, allowed in allowed_values.items():
            if col not in values or values[col] == "":
                continue
            if values[col] not in allowed:
                row_problems.append(
                    Violation(
                        family=RuleFamily.SCHEMA,
                        message=(
                            f"{col}: '{values[col]}' is not an allowed value "
                            f"(expected one of {allowed})"
                        ),
                        field=col,
                        row_number=row_number,
                    )
                )

        row_violations.extend(row_problems)

        if record is not None and not row_problems and not blank_mandatory:
            parsed_rows[row_number] = record

    return SchemaCheckResult(
        batch_violations=batch_violations,
        row_violations=row_violations,
        parsed_rows=parsed_rows,
    )
