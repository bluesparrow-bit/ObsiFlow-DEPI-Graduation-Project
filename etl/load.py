"""PostgreSQL persistence interface.

[DE] Implement every function below.  This is the only module allowed to talk to
PostgreSQL.  Translate database failures into either a retryable
:class:`TransientPipelineError` or a terminal :class:`PermanentPipelineError` so
the orchestration layer can decide whether retries are worthwhile.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg2.extensions import connection as pg_connection

from config.settings import DQConfig, Settings
from etl.contracts import PipelineRun, RunCountersSnapshot


@contextmanager
def get_connection(settings: Settings | None = None) -> Iterator[pg_connection]:
    """Yield a committed transaction or translate failures into pipeline errors."""

    raise NotImplementedError("TODO [DE]: Implement this function")
    yield  # pragma: no cover - keeps this function a context manager


def start_pipeline_run(
    run_id: str,
    dag_id: str,
    source_file: str,
    batch_id: str | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Insert (or restart) a RUNNING audit row for a run id."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def update_run_progress(
    run_id: str,
    counters: RunCountersSnapshot,
    failures_by_rule: dict[str, int],
    error_summary: str | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Persist extraction/validation counters and per-family failures."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def update_run_loaded(
    run_id: str,
    loaded_count: int,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Persist the number of upserted target rows."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def finish_pipeline_run(
    run_id: str,
    status: str,
    error_summary: str | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Finalize a run with a terminal status and computed duration."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def load_transactions(
    records: list[dict[str, Any]],
    run_id: str,
    source_file: str,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> int:
    """Upsert transformed rows into ``target.transactions`` idempotently.

    ``ON CONFLICT (transaction_id) DO UPDATE`` must guarantee that retries and
    batch replays converge on the same target state without duplicate primary
    keys.
    """

    raise NotImplementedError("TODO [DE]: Implement this function")


def fetch_reference_account_ids(
    config: DQConfig | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> set[str]:
    """Return all reference account ids for the referential-integrity rule."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def fetch_pipeline_run(
    run_id: str,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> PipelineRun | None:
    """Load one pipeline run as a validated contract."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def fetch_metrics_snapshots(
    *,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Aggregate per-DAG telemetry used by the Prometheus exporter."""

    raise NotImplementedError("TODO [DE]: Implement this function")
