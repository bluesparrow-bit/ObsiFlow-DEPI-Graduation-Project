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

    Comparison must be case-insensitive and whitespace-normalized.  Blank keys
    are ignored here and reported by the null rule.  Callers may pass a database
    snapshot or an in-memory fixture, keeping the rule unit-testable without I/O.
    """

    normalized_valid_keys = {key.strip().upper() for key in valid_keys}
    violations: list[Violation] = []
    for row_num, (_, row) in enumerate(frame.iterrows(), 2):
        raw_key = row[foreign_key]
        if pd.isna(raw_key) or not str(raw_key).strip():
            continue

        normalized_key = str(raw_key).strip().upper()
        if normalized_key not in normalized_valid_keys:
            violations.append(
                Violation(
                    family=RuleFamily.REFERENTIAL,
                    field=foreign_key,
                    row_number=row_num,
                    message=f"FOREIGN KEY constraint failed for key: {normalized_key}",
                )
            )
    return violations
