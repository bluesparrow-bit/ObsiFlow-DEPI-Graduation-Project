"""Shared data contracts and centralized run counters.

This module is dependency-light on purpose: every other package (ETL, validation,
orchestration, monitoring, tests) may import it.  It must never import Airflow,
pandas, or psycopg2.
"""

from __future__ import annotations

import threading
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RuleFamily(StrEnum):
    """The six data-quality rule families enforced by the platform."""

    NULL = "null"
    DUPLICATE = "duplicate"
    SCHEMA = "schema"
    ROW_COUNT = "row_count"
    RANGE = "range"
    REFERENTIAL = "referential"


ALL_RULE_FAMILIES: tuple[RuleFamily, ...] = tuple(RuleFamily)


class RunStatus(StrEnum):
    """Lifecycle states persisted in ``observability.pipeline_runs``."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PipelineError(Exception):
    """Base class for all pipeline failures."""


class PermanentPipelineError(PipelineError):
    """Failure that will not heal on retry (bad contract, missing schema, ...)."""


class TransientPipelineError(PipelineError):
    """Failure that may heal on retry (network blip, database restart, ...)."""


class TransactionRecord(BaseModel):
    """The canonical, validated transaction contract.

    Parsing is deliberately strict: no thousands separators, no currency symbols,
    and dates must be ISO-8601.  This lets the schema rule detect type drift
    instead of silently coercing corrupt source values.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=64)
    account_id: str = Field(min_length=1, max_length=32)
    txn_type: str = Field(min_length=1, max_length=32)
    amount: Decimal
    currency: str = Field(min_length=3, max_length=3)
    txn_date: date
    channel: str = Field(min_length=1, max_length=32)
    status: str = Field(min_length=1, max_length=32)

    @field_validator(
        "transaction_id",
        "account_id",
        "txn_type",
        "currency",
        "channel",
        "status",
        mode="before",
    )
    @classmethod
    def _reject_blank_strings(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("must not be null")
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("must not be blank")
            return stripped
        return value

    @field_validator("amount", mode="before")
    @classmethod
    def _parse_amount(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("amount must be numeric")
        if isinstance(value, Decimal):
            amount = value
        elif isinstance(value, int | float):
            amount = Decimal(str(value))
        elif isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("amount must not be blank")
            if "," in cleaned or any(symbol in cleaned for symbol in ("$", "€", "£")):
                raise ValueError("amount must be a plain decimal number")
            try:
                amount = Decimal(cleaned)
            except InvalidOperation as exc:
                raise ValueError("amount must be a plain decimal number") from exc
        else:
            raise ValueError("amount must be numeric")

        if not amount.is_finite():
            raise ValueError("amount must be finite")
        return amount

    @field_validator("txn_date", mode="before")
    @classmethod
    def _parse_iso_date(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            raise ValueError("txn_date must be a date, not a datetime")
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("txn_date must not be blank")
            try:
                return date.fromisoformat(cleaned)
            except ValueError as exc:
                raise ValueError("txn_date must be an ISO-8601 date (YYYY-MM-DD)") from exc
        raise ValueError("txn_date must be a date")


class Violation(BaseModel):
    """A single row-level rule failure with an audit-friendly reason."""

    model_config = ConfigDict(frozen=True)

    family: RuleFamily
    message: str
    field: str | None = None
    row_number: int | None = Field(default=None, ge=1)


class BatchViolation(BaseModel):
    """A batch-level rule failure (row count, missing columns, ...)."""

    model_config = ConfigDict(frozen=True)

    family: RuleFamily
    message: str
    blocking: bool = True


class RejectedRow(BaseModel):
    """A source row routed to quarantine together with every failure found."""

    row_number: int = Field(ge=1)
    transaction_id: str | None = None
    raw: dict[str, Any]
    violations: list[Violation] = Field(min_length=1)

    @property
    def rule_families(self) -> list[str]:
        return sorted({violation.family.value for violation in self.violations})

    @property
    def failure_reasons(self) -> list[dict[str, str | None]]:
        return [
            {
                "family": violation.family.value,
                "field": violation.field,
                "message": violation.message,
            }
            for violation in self.violations
        ]


class BatchValidationResult(BaseModel):
    """The complete split of a batch into valid and quarantined records."""

    source_file: str
    total_rows: int = Field(ge=0)
    valid_records: list[TransactionRecord] = Field(default_factory=list)
    rejected_rows: list[RejectedRow] = Field(default_factory=list)
    batch_violations: list[BatchViolation] = Field(default_factory=list)
    failures_by_family: dict[str, int] = Field(default_factory=dict)
    rejection_rate: float = Field(ge=0, le=1)
    is_blocking: bool = False
    rejection_threshold_exceeded: bool = False


class RunCountersSnapshot(BaseModel):
    """Immutable point-in-time view of a pipeline run's counters."""

    extracted: int = Field(default=0, ge=0)
    valid: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    loaded: int = Field(default=0, ge=0)

    @property
    def rejection_rate(self) -> float:
        if self.extracted == 0:
            return 0.0
        return round(self.rejected / self.extracted, 6)


class RunCounters:
    """Thread-safe, centralized counters for one pipeline invocation.

    Airflow retries and any future parallel operators increment the same instance;
    every persistence call reads a consistent snapshot instead of recomputing
    counts from several sources.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._extracted = 0
        self._valid = 0
        self._rejected = 0
        self._loaded = 0

    def reset(self) -> None:
        with self._lock:
            self._extracted = self._valid = self._rejected = self._loaded = 0

    def set_extracted(self, count: int) -> None:
        with self._lock:
            self._extracted = max(count, 0)

    def add_valid(self, count: int) -> None:
        with self._lock:
            self._valid += max(count, 0)

    def add_rejected(self, count: int) -> None:
        with self._lock:
            self._rejected += max(count, 0)

    def add_loaded(self, count: int) -> None:
        with self._lock:
            self._loaded += max(count, 0)

    def snapshot(self) -> RunCountersSnapshot:
        with self._lock:
            return RunCountersSnapshot(
                extracted=self._extracted,
                valid=self._valid,
                rejected=self._rejected,
                loaded=self._loaded,
            )


class PipelineRun(BaseModel):
    """Persisted execution metadata for one pipeline run."""

    run_id: str
    dag_id: str
    batch_id: str | None = None
    source_file: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    records_extracted: int = Field(default=0, ge=0)
    records_valid: int = Field(default=0, ge=0)
    records_loaded: int = Field(default=0, ge=0)
    records_rejected: int = Field(default=0, ge=0)
    rejection_rate: float = Field(default=0.0, ge=0, le=1)
    failures_by_rule: dict[str, int] = Field(default_factory=dict)
    error_summary: str | None = None
