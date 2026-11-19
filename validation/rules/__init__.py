"""Data-quality rule implementations grouped by rule family."""

from validation.rules.duplicate_check import find_duplicates
from validation.rules.null_check import find_null_violations
from validation.rules.range_check import check_ranges, check_row_count
from validation.rules.referential_check import check_referential
from validation.rules.schema_check import SchemaCheckResult, check_schema

__all__ = [
    "SchemaCheckResult",
    "check_ranges",
    "check_referential",
    "check_row_count",
    "check_schema",
    "find_duplicates",
    "find_null_violations",
]
