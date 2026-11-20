"""Rule 2: duplicate detection on the transaction business key."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from etl.contracts import RuleFamily, Violation

_FIRST_DATA_ROW = 2


def _clean_key(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def find_duplicates(
    frame: pd.DataFrame,
    key_column: str = "transaction_id",
    policy: Literal["keep_first", "keep_last"] = "keep_first",
) -> list[Violation]:
    """Flag duplicate business keys according to the configured retention policy.

    Blank keys must be ignored here because they are already reported by the null
    rule; this keeps failure counts from being double-inflated for one defect.
    """

    if policy not in ("keep_first", "keep_last"):
        raise ValueError(f"Unsupported duplicate policy: {policy!r}")

    # Missing key column is a batch-level schema failure, handled elsewhere.
    if key_column not in frame.columns:
        return []

    keys = [_clean_key(v) for v in frame[key_column].tolist()]
    positions = range(len(keys))
    if policy == "keep_last":
        positions = reversed(positions)

    kept_row: dict[str, int] = {}
    violations: list[Violation] = []

    for position in positions:
        key = keys[position]
        if not key:
            continue

        row_number = position + _FIRST_DATA_ROW
        if key not in kept_row:
            kept_row[key] = row_number
            continue

        retained = "first" if policy == "keep_first" else "last"
        violations.append(
            Violation(
                family=RuleFamily.DUPLICATE,
                message=(
                    f"duplicate_key: {key_column}='{key}' already appears in row "
                    f"{kept_row[key]} (policy keeps the {retained} occurrence)"
                ),
                field=key_column,
                row_number=row_number,
            )
        )

    violations.sort(key=lambda v: v.row_number or 0)
    return violations
