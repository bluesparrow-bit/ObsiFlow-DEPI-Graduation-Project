"""Rule 2: duplicate detection on the transaction business key."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from etl.contracts import RuleFamily, Violation


def _normalize_key(value: object) -> str:
    return str(value).strip().upper()


def find_duplicates(
    frame: pd.DataFrame,
    key_column: str = "transaction_id",
    policy: Literal["keep_first", "keep_last"] = "keep_first",
) -> list[Violation]:
    """Flag duplicate business keys according to the configured retention policy.

    Blank keys are ignored here because they are already reported by the null
    rule; this keeps failure counts from being double-inflated for one defect.
    """

    if frame.empty or key_column not in frame.columns:
        return []

    keys = [_normalize_key(value) for value in frame[key_column].tolist()]
    groups: dict[str, list[int]] = {}
    for position, key in enumerate(keys):
        if not key:
            continue
        groups.setdefault(key, []).append(position)

    violations: list[Violation] = []
    for key, positions in groups.items():
        if len(positions) < 2:
            continue
        keep_position = positions[0] if policy == "keep_first" else positions[-1]
        for position in positions:
            if position == keep_position:
                continue
            violations.append(
                Violation(
                    family=RuleFamily.DUPLICATE,
                    field=key_column,
                    row_number=position + 2,
                    message=(
                        f"Duplicate value '{key}' for business key '{key_column}' (policy={policy})"
                    ),
                )
            )

    return sorted(violations, key=lambda violation: violation.row_number or 0)
