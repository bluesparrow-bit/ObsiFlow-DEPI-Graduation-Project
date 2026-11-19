"""Quarantine persistence interface.

[DQ] Implement idempotent dead-letter writes.  One quarantine row must exist per
rejected source row, keyed by ``(run_id, row_number)``, preserving the raw
payload and every failure reason.
"""

from __future__ import annotations

from typing import Any

from psycopg2.extensions import connection as pg_connection

from config.settings import DQConfig, Settings
from etl.contracts import RejectedRow


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

    ``ON CONFLICT (run_id, row_number) DO UPDATE`` must make retries idempotent:
    a failed Airflow task can re-quarantine the same batch without duplicates.
    """

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")


def fetch_quarantined_rows(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Return quarantined rows for a run, ordered by source row number."""

    raise NotImplementedError("TODO [DQ]: Implement this validation rule")
