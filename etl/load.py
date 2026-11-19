"""Idempotent PostgreSQL persistence for runs, quarantine, and target rows.

Only this module talks to PostgreSQL.  Every database error is translated into
either a retryable :class:`TransientPipelineError` or a terminal
:class:`PermanentPipelineError`, which is what the Airflow fail-fast wrapper
uses to decide whether retries are worthwhile.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2 import InterfaceError, OperationalError, sql
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import Json, RealDictCursor, execute_values

from config.settings import DQConfig, Settings, get_settings
from etl.contracts import (
    PermanentPipelineError,
    PipelineRun,
    RunCountersSnapshot,
    TransientPipelineError,
)

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TRANSIENT_ERRORS = (OperationalError, InterfaceError)
_PERMANENT_ERRORS = (psycopg2.IntegrityError, psycopg2.DataError, psycopg2.ProgrammingError)


def _qualified_identifier(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER_PATTERN.fullmatch(part) for part in parts):
        raise PermanentPipelineError(
            f"Unsafe or unqualified SQL identifier '{name}'; expected schema.table"
        )
    return name


@contextmanager
def get_connection(settings: Settings | None = None) -> Iterator[pg_connection]:
    """Yield a committed transaction or translate failures into pipeline errors."""

    resolved = settings or get_settings()
    try:
        connection = psycopg2.connect(resolved.database_dsn)
    except _TRANSIENT_ERRORS as exc:
        raise TransientPipelineError(f"Could not connect to PostgreSQL: {exc}") from exc

    try:
        yield connection
        connection.commit()
    except _TRANSIENT_ERRORS as exc:
        connection.rollback()
        raise TransientPipelineError(f"Transient PostgreSQL failure: {exc}") from exc
    except _PERMANENT_ERRORS as exc:
        connection.rollback()
        raise PermanentPipelineError(f"Permanent PostgreSQL failure: {exc}") from exc
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@contextmanager
def _using(conn: pg_connection | None, settings: Settings | None = None) -> Iterator[pg_connection]:
    if conn is not None:
        yield conn
        return
    with get_connection(settings) as managed:
        yield managed


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

    query = """
        INSERT INTO observability.pipeline_runs
            (run_id, dag_id, batch_id, source_file, status, started_at)
        VALUES (%s, %s, %s, %s, 'RUNNING', now())
        ON CONFLICT (run_id) DO UPDATE SET
            dag_id = EXCLUDED.dag_id,
            batch_id = EXCLUDED.batch_id,
            source_file = EXCLUDED.source_file,
            status = 'RUNNING',
            started_at = LEAST(observability.pipeline_runs.started_at, now()),
            finished_at = NULL,
            duration_seconds = NULL,
            records_extracted = 0,
            records_valid = 0,
            records_loaded = 0,
            records_rejected = 0,
            rejection_rate = 0,
            failures_by_rule = '{}'::JSONB,
            error_summary = NULL
    """
    with _using(conn, settings) as active, active.cursor() as cursor:
        cursor.execute(query, (run_id, dag_id, batch_id, source_file))
    logger.info("Started pipeline run %s for %s", run_id, source_file)


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

    query = """
        UPDATE observability.pipeline_runs
        SET records_extracted = %s,
            records_valid = %s,
            records_rejected = %s,
            rejection_rate = %s,
            failures_by_rule = %s,
            error_summary = COALESCE(%s, error_summary)
        WHERE run_id = %s
    """
    with _using(conn, settings) as active, active.cursor() as cursor:
        cursor.execute(
            query,
            (
                counters.extracted,
                counters.valid,
                counters.rejected,
                counters.rejection_rate,
                Json(failures_by_rule),
                error_summary,
                run_id,
            ),
        )


def update_run_loaded(
    run_id: str,
    loaded_count: int,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Persist the number of upserted target rows."""

    with _using(conn, settings) as active, active.cursor() as cursor:
        cursor.execute(
            "UPDATE observability.pipeline_runs SET records_loaded = %s WHERE run_id = %s",
            (loaded_count, run_id),
        )


def finish_pipeline_run(
    run_id: str,
    status: str,
    error_summary: str | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> None:
    """Finalize a run with a terminal status and computed duration."""

    if status not in {"SUCCESS", "FAILED"}:
        raise PermanentPipelineError(f"Invalid terminal pipeline status: {status}")

    query = """
        UPDATE observability.pipeline_runs
        SET status = %s,
            finished_at = now(),
            duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
            error_summary = CASE
                WHEN %s = 'SUCCESS' THEN NULL
                ELSE COALESCE(%s, error_summary)
            END
        WHERE run_id = %s
    """
    with _using(conn, settings) as active, active.cursor() as cursor:
        cursor.execute(query, (status, status, error_summary, run_id))
        if cursor.rowcount == 0:
            raise PermanentPipelineError(f"Pipeline run does not exist: {run_id}")
    logger.info("Finished pipeline run %s with status %s", run_id, status)


def load_transactions(
    records: list[dict[str, Any]],
    run_id: str,
    source_file: str,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> int:
    """Upsert transformed rows into ``target.transactions`` idempotently.

    ``ON CONFLICT (transaction_id) DO UPDATE`` guarantees that retries and batch
    replays converge on the same target state without duplicate primary keys.
    """

    if not records:
        return 0

    insert_sql = """
        INSERT INTO target.transactions
            (transaction_id, account_id, txn_type, amount, currency, txn_date,
             channel, status, source_file, load_run_id)
        VALUES %s
        ON CONFLICT (transaction_id) DO UPDATE SET
            account_id = EXCLUDED.account_id,
            txn_type = EXCLUDED.txn_type,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            txn_date = EXCLUDED.txn_date,
            channel = EXCLUDED.channel,
            status = EXCLUDED.status,
            source_file = EXCLUDED.source_file,
            load_run_id = EXCLUDED.load_run_id,
            updated_at = now()
    """
    values = [
        (
            row["transaction_id"],
            row["account_id"],
            row["txn_type"],
            row["amount"],
            row["currency"],
            row["txn_date"],
            row["channel"],
            row["status"],
            source_file,
            run_id,
        )
        for row in records
    ]

    with _using(conn, settings) as active, active.cursor() as cursor:
        execute_values(cursor, insert_sql, values, page_size=500)

    logger.info("Upserted %d rows into target.transactions for run %s", len(values), run_id)
    return len(values)


def fetch_reference_account_ids(
    config: DQConfig | None = None,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> set[str]:
    """Return all reference account ids for the referential-integrity rule."""

    if config is None:
        from config.settings import load_dq_config

        config = load_dq_config()

    table = _qualified_identifier(config.referential.reference_table)
    reference_key = config.referential.reference_key
    if not _IDENTIFIER_PATTERN.fullmatch(reference_key):
        raise PermanentPipelineError(f"Unsafe reference key: {reference_key}")

    query = sql.SQL("SELECT {column} FROM {table}").format(
        column=sql.Identifier(reference_key),
        table=sql.SQL(table),
    )
    with _using(conn, settings) as active, active.cursor() as cursor:
        cursor.execute(query)
        return {str(row[0]).strip().upper() for row in cursor.fetchall()}


def fetch_pipeline_run(
    run_id: str,
    *,
    conn: pg_connection | None = None,
    settings: Settings | None = None,
) -> PipelineRun | None:
    """Load one pipeline run as a validated contract."""

    with _using(conn, settings) as active, active.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT * FROM observability.pipeline_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return PipelineRun.model_validate(dict(row))


def fetch_metrics_snapshots(
    *,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Aggregate per-DAG telemetry used by the Prometheus exporter.

    The query is intentionally a single round trip so the exporter can refresh
    frequently without holding connections open or racing several snapshots.
    """

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (dag_id)
                dag_id,
                status AS latest_status,
                started_at AS latest_started_at,
                finished_at AS latest_finished_at,
                duration_seconds AS latest_duration_seconds,
                rejection_rate AS latest_rejection_rate,
                failures_by_rule AS latest_failures_by_rule,
                records_extracted AS latest_extracted,
                records_loaded AS latest_loaded,
                records_rejected AS latest_rejected
            FROM observability.pipeline_runs
            ORDER BY dag_id, started_at DESC, finished_at DESC NULLS LAST
        ),
        totals AS (
            SELECT
                dag_id,
                COUNT(*) AS total_runs,
                COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_runs,
                COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_runs,
                COUNT(*) FILTER (WHERE status = 'RUNNING') AS running_runs,
                COALESCE(SUM(records_extracted), 0) AS extracted_total,
                COALESCE(SUM(records_loaded), 0) AS loaded_total,
                COALESCE(SUM(records_rejected), 0) AS rejected_total,
                MAX(finished_at) FILTER (WHERE status = 'SUCCESS') AS last_success_at,
                MAX(COALESCE(finished_at, started_at)) AS last_reported_at
            FROM observability.pipeline_runs
            GROUP BY dag_id
        )
        SELECT t.*, l.*
        FROM totals AS t
        JOIN latest AS l USING (dag_id)
        ORDER BY t.dag_id
    """
    with get_connection(settings) as active, active.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
