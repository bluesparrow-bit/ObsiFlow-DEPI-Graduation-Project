"""Rule 1: mandatory non-null field validation."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from etl.contracts import Violation


def find_null_violations(
    frame: pd.DataFrame,
    mandatory_fields: Sequence[str],
) -> list[Violation]:
    """Return a violation for every blank mandatory field.

    The CSV reader preserves blanks as empty strings, so ``"   "`` must be
    treated exactly like a missing value.  Row numbers are 1-based plus the
    header row.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
