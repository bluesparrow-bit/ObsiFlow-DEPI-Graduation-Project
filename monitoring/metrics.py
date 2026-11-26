"""Single-source Prometheus metrics emitter for the ObsiFlow platform.

The exporter intentionally derives every value from PostgreSQL.  Airflow workers
may be restarted at any time, so in-process counters would reset and silently
lie.  ``pipeline_runs`` is the source of truth; this module projects it into the
Prometheus registry served on ``OBSIFLOW_METRICS_PORT``.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime
from typing import Any

from prometheus_client import CollectorRegistry, Gauge, generate_latest, start_http_server

from config.settings import Settings, get_settings
from etl.contracts import ALL_RULE_FAMILIES
from etl.load import fetch_metrics_snapshots

logger = logging.getLogger(__name__)

REGISTRY = CollectorRegistry()

RUNS_TOTAL = Gauge(
    "pipeline_runs_total",
    "Pipeline runs recorded in PostgreSQL, grouped by terminal status.",
    ["dag_id", "status"],
    registry=REGISTRY,
)
RUNS_IN_FLIGHT = Gauge(
    "pipeline_runs_in_flight",
    "Pipeline runs currently in RUNNING state.",
    ["dag_id"],
    registry=REGISTRY,
)
RECORDS_PROCESSED = Gauge(
    "pipeline_records_processed_total",
    "Total extracted records across all runs.",
    ["dag_id"],
    registry=REGISTRY,
)
RECORDS_LOADED = Gauge(
    "pipeline_records_loaded_total",
    "Total records upserted into the target table across all runs.",
    ["dag_id"],
    registry=REGISTRY,
)
RECORDS_REJECTED = Gauge(
    "pipeline_records_rejected_total",
    "Total records quarantined across all runs.",
    ["dag_id"],
    registry=REGISTRY,
)
DQ_FAILURES = Gauge(
    "pipeline_dq_failures_total",
    "Data-quality failures in the most recent run, by rule family.",
    ["dag_id", "rule_family"],
    registry=REGISTRY,
)
LAST_SUCCESS = Gauge(
    "pipeline_last_success_timestamp_seconds",
    "Unix timestamp of the most recent successful run (0 if none).",
    ["dag_id"],
    registry=REGISTRY,
)
LAST_REPORTED = Gauge(
    "pipeline_last_reported_timestamp_seconds",
    "Unix timestamp of the most recent run report of any status (0 if none).",
    ["dag_id"],
    registry=REGISTRY,
)
LAST_RUN_STATUS = Gauge(
    "pipeline_last_run_status",
    "One-hot indicator of the most recent run status.",
    ["dag_id", "status"],
    registry=REGISTRY,
)
REJECTION_RATE = Gauge(
    "pipeline_rejection_rate",
    "Rejected fraction of the most recent run.",
    ["dag_id"],
    registry=REGISTRY,
)
RUN_DURATION = Gauge(
    "pipeline_last_run_duration_seconds",
    "Wall-clock duration of the most recent run.",
    ["dag_id"],
    registry=REGISTRY,
)
DB_UP = Gauge(
    "pipeline_db_up",
    "1 when the exporter can read PostgreSQL, 0 otherwise.",
    registry=REGISTRY,
)
EXPORTER_REFRESH = Gauge(
    "pipeline_exporter_last_refresh_timestamp_seconds",
    "Unix timestamp of the last successful exporter refresh.",
    registry=REGISTRY,
)

_LABELLED_GAUGES = (
    RUNS_TOTAL,
    RUNS_IN_FLIGHT,
    RECORDS_PROCESSED,
    RECORDS_LOADED,
    RECORDS_REJECTED,
    DQ_FAILURES,
    LAST_SUCCESS,
    LAST_REPORTED,
    LAST_RUN_STATUS,
    REJECTION_RATE,
    RUN_DURATION,
)


def _epoch(value: datetime | None) -> float:
    return value.timestamp() if value is not None else 0.0


def _as_dict(row: dict[str, Any], key: str) -> dict[str, int]:
    raw = row.get(key) or {}
    return {str(name): int(count) for name, count in raw.items()}


def _publish_snapshot(snapshot: dict[str, Any]) -> None:
    dag_id = str(snapshot["dag_id"])

    RUNS_TOTAL.labels(dag_id, "success").set(int(snapshot.get("success_runs") or 0))
    RUNS_TOTAL.labels(dag_id, "failed").set(int(snapshot.get("failed_runs") or 0))
    RUNS_TOTAL.labels(dag_id, "running").set(int(snapshot.get("running_runs") or 0))
    RUNS_IN_FLIGHT.labels(dag_id).set(int(snapshot.get("running_runs") or 0))

    RECORDS_PROCESSED.labels(dag_id).set(float(snapshot.get("extracted_total") or 0))
    RECORDS_LOADED.labels(dag_id).set(float(snapshot.get("loaded_total") or 0))
    RECORDS_REJECTED.labels(dag_id).set(float(snapshot.get("rejected_total") or 0))

    LAST_SUCCESS.labels(dag_id).set(_epoch(snapshot.get("last_success_at")))
    LAST_REPORTED.labels(dag_id).set(_epoch(snapshot.get("last_reported_at")))

    latest_status = str(snapshot.get("latest_status") or "").upper()
    for status in ("success", "failed", "running"):
        LAST_RUN_STATUS.labels(dag_id, status).set(1.0 if latest_status == status.upper() else 0.0)

    REJECTION_RATE.labels(dag_id).set(float(snapshot.get("latest_rejection_rate") or 0.0))
    RUN_DURATION.labels(dag_id).set(float(snapshot.get("latest_duration_seconds") or 0.0))

    failures = _as_dict(snapshot, "latest_failures_by_rule")
    for family in ALL_RULE_FAMILIES:
        DQ_FAILURES.labels(dag_id, family.value).set(float(failures.get(family.value, 0)))


def refresh_metrics_from_db(settings: Settings | None = None) -> bool:
    """Refresh every gauge from PostgreSQL.

    Returns:
        ``True`` when the database was reachable, ``False`` otherwise.  A failed
        refresh never raises: the exporter must stay alive to report ``db_up=0``.
    """

    try:
        snapshots = fetch_metrics_snapshots(settings=settings)
    except Exception as exc:
        DB_UP.set(0)
        logger.warning("Metrics refresh failed; PostgreSQL is unreachable: %s", exc)
        return False

    for gauge in _LABELLED_GAUGES:
        gauge.clear()
    for snapshot in snapshots:
        _publish_snapshot(snapshot)

    DB_UP.set(1)
    EXPORTER_REFRESH.set(time.time())
    logger.debug("Refreshed metrics for %d DAG(s)", len(snapshots))
    return True


def emit_run_metrics(run_id: str | None = None, settings: Settings | None = None) -> bool:
    """DAG-friendly entry point that refreshes DB-backed metrics.

    ``run_id`` is accepted for call-site symmetry and logging; the database is
    always the source of truth.
    """

    logger.info("Emitting telemetry for run %s", run_id or "<all>")
    return refresh_metrics_from_db(settings=settings)


def render_metrics() -> str:
    """Return the Prometheus text exposition (used by tests and diagnostics)."""

    return generate_latest(REGISTRY).decode("utf-8")


def main() -> None:
    """Run the long-lived exporter endpoint."""

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(settings.metrics_port, registry=REGISTRY)
    logger.info("Prometheus exporter listening on :%d", settings.metrics_port)

    stop_event = threading.Event()

    def _stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while not stop_event.is_set():
        refresh_metrics_from_db(settings)
        stop_event.wait(settings.metrics_refresh_interval_seconds)

    logger.info("Prometheus exporter stopped")


if __name__ == "__main__":  # pragma: no cover - process entry point
    main()
