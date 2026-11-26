"""Rule 4/5: batch row-count bounds and numeric/date range validation.

The specification groups row-count bounds and value ranges under one rule file.
The engine treats row-count violations as blocking batch failures and value
range violations as per-row quarantine candidates.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from etl.contracts import BatchViolation, RuleFamily, Violation


def check_row_count(total_rows: int, min_rows: int, max_rows: int) -> list[BatchViolation]:
    """Return a blocking violation when the batch falls outside configured bounds."""

    violations: list[BatchViolation] = []
    if total_rows < min_rows:
        violations.append(
            BatchViolation(
                family=RuleFamily.ROW_COUNT,
                message=f"Batch rows = {total_rows}; the minimum is {min_rows}",
            )
        )

    if total_rows > max_rows:
        violations.append(
            BatchViolation(
                family=RuleFamily.ROW_COUNT,
                message=f"Batch rows = {total_rows}; the maximum is {max_rows}",
            )
        )

    return violations


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
    violations: list[Violation] = []
    for row_num, (_, row) in enumerate(frame.iterrows(), 2):
        if "amount" in frame.columns:
            amount = pd.to_numeric(row["amount"], errors="coerce")
            if not pd.isna(amount) and amount > amount_max:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="amount",
                        row_number=row_num,
                        message=f"Amount {amount} is higher than the max amount: {amount_max}",
                    )
                )
            if not pd.isna(amount) and amount < amount_min:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="amount",
                        row_number=row_num,
                        message=f"Amount {amount} is less than the min amount: {amount_min}",
                    )
                )

        if "txn_date" in frame.columns:
            raw_date = row["txn_date"]
            if isinstance(raw_date, datetime):
                continue
            if isinstance(raw_date, date):
                transaction_date = raw_date
            elif isinstance(raw_date, str):
                cleaned_date = raw_date.strip()
                try:
                    transaction_date = date.fromisoformat(cleaned_date)
                except ValueError:
                    continue
                if transaction_date.isoformat() != cleaned_date:
                    continue
            else:
                continue

            if transaction_date > date_max:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="txn_date",
                        row_number=row_num,
                        message=(
                            f"Date {transaction_date} is higher than the maximum date: {date_max}"
                        ),
                    )
                )

            if transaction_date < date_min:
                violations.append(
                    Violation(
                        family=RuleFamily.RANGE,
                        field="txn_date",
                        row_number=row_num,
                        message=(
                            f"Date {transaction_date} is less than the minimum date: {date_min}"
                        ),
                    )
                )

    return violations
