"""Rule 2: duplicate detection on the transaction business key."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from etl.contracts import Violation


def find_duplicates(
    frame: pd.DataFrame,
    key_column: str = "transaction_id",
    policy: Literal["keep_first", "keep_last"] = "keep_first",
) -> list[Violation]:
    """Flag duplicate business keys according to the configured retention policy.

    Blank keys must be ignored here because they are already reported by the null
    rule; this keeps failure counts from being double-inflated for one defect.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
