"""Rule 4/5: batch row-count bounds and numeric/date range validation.

The specification groups row-count bounds and value ranges under one rule file.
The engine treats row-count violations as blocking batch failures and value
range violations as per-row quarantine candidates.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from etl.contracts import BatchViolation, Violation


def check_row_count(total_rows: int, min_rows: int, max_rows: int) -> list[BatchViolation]:
    """Return a blocking violation when the batch falls outside configured bounds."""

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")


def check_ranges(
    frame: pd.DataFrame,
    amount_min: float,
    amount_max: float,
    date_min: date,
    date_max: date,
) -> list[Violation]:
    """Validate inclusive amount and transaction-date bounds.

    Unparseable values must be left to the schema rule so one corrupt cell does
    not produce a misleading range failure as well.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
