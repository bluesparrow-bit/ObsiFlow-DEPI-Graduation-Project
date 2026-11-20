"""Rule 1: mandatory non-null field validation."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from etl.contracts import RuleFamily, Violation

# Row 1 is the CSV header, so the first data row is row 2.
_FIRST_DATA_ROW = 2


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip() == ""


def find_null_violations(
    frame: pd.DataFrame,
    mandatory_fields: Sequence[str],
) -> list[Violation]:
    """Return a violation for every blank mandatory field.

    The CSV reader preserves blanks as empty strings, so ``"   "`` must be
    treated exactly like a missing value.  Row numbers are 1-based plus the
    header row.
    """

    violations: list[Violation] = []

    for field_name in mandatory_fields:
        # A missing column is a batch-level schema failure, not a per-row null.
        if field_name not in frame.columns:
            continue

        for position, value in enumerate(frame[field_name].tolist()):
            if _is_blank(value):
                violations.append(
                    Violation(
                        family=RuleFamily.NULL,
                        message=f"missing_field: '{field_name}' is null or blank",
                        field=field_name,
                        row_number=position + _FIRST_DATA_ROW,
                    )
                )

    # Stable sort keeps the mandatory-field order within the same row.
    violations.sort(key=lambda v: v.row_number or 0)
    return violations
