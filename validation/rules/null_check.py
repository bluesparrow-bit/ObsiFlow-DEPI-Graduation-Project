"""Rule 1: mandatory non-null field validation."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import pandas as pd

from etl.contracts import RuleFamily, Violation


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return not str(value).strip()


def find_null_violations(
    frame: pd.DataFrame,
    mandatory_fields: Sequence[str],
) -> list[Violation]:
    """Return a violation for every blank mandatory field.

    The CSV reader preserves blanks as empty strings, so ``"   "`` is treated
    exactly like a missing value.  Row numbers are 1-based plus the header row.
    """

    violations: list[Violation] = []
    if frame.empty:
        return violations

    for field_name in mandatory_fields:
        if field_name not in frame.columns:
            continue
        for row_number, value in enumerate(frame[field_name].tolist(), start=2):
            if _is_blank(value):
                violations.append(
                    Violation(
                        family=RuleFamily.NULL,
                        field=field_name,
                        row_number=row_number,
                        message=f"Mandatory field '{field_name}' is null or blank",
                    )
                )
    return violations
