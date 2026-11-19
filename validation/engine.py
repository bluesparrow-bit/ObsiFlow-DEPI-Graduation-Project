"""Orchestration of all six data-quality rule families.

The engine is pure: it takes a dataframe, a validated rule configuration, and a
collection of reference keys, and returns a deterministic valid/invalid split.
Database access happens in callers, which keeps every rule unit-testable.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Collection
from typing import Any

import pandas as pd

from config.settings import DQConfig
from etl.contracts import (
    ALL_RULE_FAMILIES,
    BatchValidationResult,
    RejectedRow,
    RuleFamily,
    TransactionRecord,
    Violation,
)
from validation.rules import (
    check_ranges,
    check_referential,
    check_row_count,
    check_schema,
    find_duplicates,
    find_null_violations,
)

logger = logging.getLogger(__name__)


def _raw_value(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _raw_record(frame: pd.DataFrame, position: int) -> dict[str, Any]:
    return {str(column): _raw_value(frame.iloc[position][column]) for column in frame.columns}


def validate_batch(
    frame: pd.DataFrame,
    config: DQConfig,
    reference_ids: Collection[str],
    source_file: str = "<memory>",
) -> BatchValidationResult:
    """Evaluate the six rule families and split valid from invalid rows.

    Policy summary:
    * Missing required columns and out-of-bounds row counts are blocking.
    * A row with any violation is quarantined; all other rows remain loadable.
    * The rejection-rate threshold is alert-only, not a load gate.
    """

    total_rows = len(frame)
    batch_violations = list(
        check_row_count(total_rows, config.batch.min_rows, config.batch.max_rows)
    )

    schema_result = check_schema(
        frame,
        required_columns=config.schema_rules.required_columns,
        allowed_values=config.schema_rules.allowed_values,
    )
    batch_violations.extend(schema_result.batch_violations)
    schema_is_usable = not schema_result.batch_violations

    violations_by_row: dict[int, list[Violation]] = defaultdict(list)

    if schema_is_usable:
        row_violations = [
            *find_null_violations(frame, config.fields.mandatory),
            *find_duplicates(
                frame,
                key_column=config.batch.duplicate_key,
                policy=config.batch.duplicate_policy,
            ),
            *schema_result.row_violations,
            *check_ranges(
                frame,
                amount_min=config.fields.amount.min,
                amount_max=config.fields.amount.max,
                date_min=config.fields.date.min,
                date_max=config.fields.date.max,
            ),
            *check_referential(
                frame,
                foreign_key=config.referential.foreign_key,
                valid_keys=reference_ids,
            ),
        ]
        for violation in row_violations:
            if violation.row_number is not None:
                violations_by_row[violation.row_number].append(violation)
    elif total_rows:
        missing_reason = "; ".join(v.message for v in schema_result.batch_violations)
        for position in range(total_rows):
            violations_by_row[position + 2].append(
                Violation(
                    family=RuleFamily.SCHEMA,
                    row_number=position + 2,
                    message=(
                        "Row cannot be parsed because the batch schema is invalid: "
                        f"{missing_reason}"
                    ),
                )
            )

    rejected_rows: list[RejectedRow] = []
    valid_records: list[TransactionRecord] = []
    for position in range(total_rows):
        row_number = position + 2
        row_violations = sorted(
            violations_by_row.get(row_number, []),
            key=lambda violation: (violation.family.value, violation.message),
        )
        raw = _raw_record(frame, position)
        transaction_id = _raw_value(
            frame.iloc[position][config.batch.duplicate_key]
            if config.batch.duplicate_key in frame.columns
            else None
        )

        if row_violations:
            rejected_rows.append(
                RejectedRow(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    raw=raw,
                    violations=row_violations,
                )
            )
            continue

        parsed = schema_result.parsed_rows.get(row_number)
        if parsed is None:
            rejected_rows.append(
                RejectedRow(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    raw=raw,
                    violations=[
                        Violation(
                            family=RuleFamily.SCHEMA,
                            row_number=row_number,
                            message="Row could not be parsed into the transaction contract",
                        )
                    ],
                )
            )
        else:
            valid_records.append(parsed)

    failures_by_family = {family.value: 0 for family in ALL_RULE_FAMILIES}
    for violation in batch_violations:
        failures_by_family[violation.family.value] += 1
    for row_violations in violations_by_row.values():
        for violation in row_violations:
            failures_by_family[violation.family.value] += 1

    rejection_rate = round(len(rejected_rows) / total_rows, 6) if total_rows else 0.0
    result = BatchValidationResult(
        source_file=source_file,
        total_rows=total_rows,
        valid_records=valid_records,
        rejected_rows=rejected_rows,
        batch_violations=batch_violations,
        failures_by_family=failures_by_family,
        rejection_rate=rejection_rate,
        is_blocking=any(violation.blocking for violation in batch_violations),
        rejection_threshold_exceeded=rejection_rate > config.batch.max_rejection_rate,
    )

    logger.info(
        "Validated %d rows: valid=%d rejected=%d blocking=%s rejection_rate=%.2f%%",
        total_rows,
        len(valid_records),
        len(rejected_rows),
        result.is_blocking,
        rejection_rate * 100,
    )
    return result
