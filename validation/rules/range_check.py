"""Rule 4/5: batch row-count bounds and numeric/date range validation.

The specification groups row-count bounds and value ranges under one rule file.
The engine treats row-count violations as blocking batch failures and value
range violations as per-row quarantine candidates.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd

from etl.contracts import BatchViolation, RuleFamily, Violation


def check_row_count(total_rows: int, min_rows: int, max_rows: int) -> list[BatchViolation]:
    """Return a blocking violation when the batch falls outside configured bounds."""

    if total_rows < min_rows:
        return [
            BatchViolation(
                family=RuleFamily.ROW_COUNT,
                message=f"Batch has {total_rows} rows; minimum is {min_rows}",
            )
        ]
    if total_rows > max_rows:
        return [
            BatchViolation(
                family=RuleFamily.ROW_COUNT,
                message=f"Batch has {total_rows} rows; maximum is {max_rows}",
            )
        ]
    return []


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or not str(value).strip():
        return None
    try:
        parsed = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _parse_date(value: object) -> date | None:
    if value is None or not str(value).strip():
        return None
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        return None


def check_ranges(
    frame: pd.DataFrame,
    amount_min: float,
    amount_max: float,
    date_min: date,
    date_max: date,
) -> list[Violation]:
    """Validate inclusive amount and transaction-date bounds.

    Unparseable values are left to the schema rule so one corrupt cell does not
    produce a misleading range failure as well.
    """

    violations: list[Violation] = []
    lower_amount = Decimal(str(amount_min))
    upper_amount = Decimal(str(amount_max))

    if "amount" in frame.columns:
        for position, value in enumerate(frame["amount"].tolist()):
            amount = _parse_decimal(value)
            if amount is None:
                continue
            if amount < lower_amount or amount > upper_amount:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="amount",
                        row_number=position + 2,
                        message=(
                            f"Amount {amount} is outside the allowed range "
                            f"[{lower_amount}, {upper_amount}]"
                        ),
                    )
                )

    if "txn_date" in frame.columns:
        for position, value in enumerate(frame["txn_date"].tolist()):
            txn_date = _parse_date(value)
            if txn_date is None:
                continue
            if txn_date < date_min or txn_date > date_max:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="txn_date",
                        row_number=position + 2,
                        message=(
                            f"Date {txn_date.isoformat()} is outside the allowed range "
                            f"[{date_min.isoformat()}, {date_max.isoformat()}]"
                        ),
                    )
                )

    return sorted(violations, key=lambda violation: violation.row_number or 0)
