"""Rule 6: foreign-key existence against the reference account table."""

from __future__ import annotations

from collections.abc import Collection

import pandas as pd

from etl.contracts import RuleFamily, Violation


def check_referential(
    frame: pd.DataFrame,
    foreign_key: str,
    valid_keys: Collection[str],
) -> list[Violation]:
    """Flag non-blank foreign keys that are absent from the reference set.

    Comparison is case-insensitive and whitespace-normalized.  Blank keys are
    ignored here and reported by the null rule.  Callers may pass a database
    snapshot or an in-memory fixture, keeping the rule unit-testable without I/O.
    """

    if frame.empty or foreign_key not in frame.columns:
        return []

    normalized_keys = {str(key).strip().upper() for key in valid_keys}
    violations: list[Violation] = []
    for position, value in enumerate(frame[foreign_key].tolist()):
        key = str(value).strip().upper()
        if not key:
            continue
        if key not in normalized_keys:
            violations.append(
                Violation(
                    family=RuleFamily.REFERENTIAL,
                    field=foreign_key,
                    row_number=position + 2,
                    message=(
                        f"Foreign key '{value}' in '{foreign_key}' does not exist "
                        "in the reference table"
                    ),
                )
            )
    return violations
