"""Rule 6: foreign-key existence against the reference account table."""

from __future__ import annotations

from collections.abc import Collection

import pandas as pd

from etl.contracts import Violation


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

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
