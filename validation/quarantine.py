"""Persistence of rejected records into the quarantine table."""

from __future__ import annotations

import logging
import re
from typing import Any

from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import Json, RealDictCursor, execute_values

from config.settings import DQConfig, Settings, load_dq_config
from etl.contracts import PermanentPipelineError, RejectedRow
from etl.load import get_connection

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quarantine_table(config: DQConfig) -> str:
    table = config.quarantine.table
    parts = table.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER_PATTERN.fullmatch(part) for part in parts):
        raise PermanentPipelineError(f"Unsafe quarantine table '{table}'; expected schema.table")
    return table


def _failure_reasons(rejected: RejectedRow, max_length: int) -> list[dict[str, str | None]]:
    reasons: list[dict[str, str | None]] = []
    for violation in rejected.violations:
        message = violation.message
        if len(message) > max_length:
            message = message[: max_length - 3] + "..."
        reasons.append(
            {
                "family": violation.family.value,
                "field": violation.field,
                "message": message,
            }
        )
    return reasons


def persist_rejected_rows(
    run_id: str,
    source_file: str,
    rejected_rows: list[RejectedRow],
    *,
    conn: pg_connection | None = None,
    config: DQConfig | None = None,
    settings: Settings | None = None,
) -> int:
    """Persist one quarantine row per rejected source row.

    ``ON CONFLICT (run_id, row_number) DO UPDATE`` makes retries idempotent: a
    failed Airflow task can re-quarantine the same batch without duplicates.
    """

    if not rejected_rows:
        return 0

    resolved_config = config or load_dq_config()
    table = _quarantine_table(resolved_config)
    max_length = resolved_config.quarantine.max_reason_length

    values = [
        (
            run_id,
            source_file,
            rejected.row_number,
            rejected.transaction_id,
            rejected.rule_families,
            Json(_failure_reasons(rejected, max_length)),
            Json(rejected.raw),
        )
        for rejected in rejected_rows
    ]

    insert_sql = f"""
        INSERT INTO {table}
            (run_id, source_file, row_number, transaction_id,
             rule_families, failure_reasons, raw_record)
        VALUES %s
        ON CONFLICT (run_id, row_number) DO UPDATE SET
            source_file = EXCLUDED.source_file,
            transaction_id = EXCLUDED.transaction_id,
            rule_families = EXCLUDED.rule_families,
            failure_reasons = EXCLUDED.failure_reasons,
            raw_record = EXCLUDED.raw_record
    """

    if conn is not None:
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, values, page_size=500)
    else:
        with get_connection(settings) as active, active.cursor() as cursor:
            execute_values(cursor, insert_sql, values, page_size=500)

    logger.info("Quarantined %d rows for run %s", len(values), run_id)
    return len(values)


def fetch_quarantined_rows(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Return quarantined rows for an run, ordered by source row number."""

    with get_connection(settings) as active, active.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT run_id, source_file, row_number, transaction_id,
                   rule_families, failure_reasons, raw_record, created_at
            FROM quality.error_records
            WHERE run_id = %s
            ORDER BY row_number
            """,
            (run_id,),
        )
        return [dict(row) for row in cursor.fetchall()]
