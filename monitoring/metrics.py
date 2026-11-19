"""Prometheus telemetry interface.

[OB] Implement a single exporter that derives every value from PostgreSQL.
Airflow workers may restart at any time, so in-process counters must never be the
source of truth: ``observability.pipeline_runs`` is authoritative.

Required metrics (exact names):
    pipeline_runs_total{dag_id,status}
    pipeline_runs_in_flight{dag_id}
    pipeline_records_processed_total{dag_id}
    pipeline_records_loaded_total{dag_id}
    pipeline_records_rejected_total{dag_id}
    pipeline_dq_failures_total{dag_id,rule_family}
    pipeline_last_run_status{dag_id,status}
    pipeline_last_success_timestamp_seconds{dag_id}
    pipeline_last_reported_timestamp_seconds{dag_id}
    pipeline_rejection_rate{dag_id}
    pipeline_last_run_duration_seconds{dag_id}
    pipeline_db_up
    pipeline_exporter_last_refresh_timestamp_seconds

Required alerts (see ``monitoring/alert_rules.yml``):
    PipelineRunFailed, PipelineStale, PipelineNeverReported, HighRejectionRate
"""

from __future__ import annotations

import logging
import signal
import threading

from prometheus_client import CollectorRegistry, generate_latest, start_http_server

from config.settings import Settings, get_settings

logger = logging.getLogger(__name__)

REGISTRY = CollectorRegistry()

REQUIRED_METRICS = (
    "pipeline_runs_total",
    "pipeline_runs_in_flight",
    "pipeline_records_processed_total",
    "pipeline_records_loaded_total",
    "pipeline_records_rejected_total",
    "pipeline_dq_failures_total",
    "pipeline_last_run_status",
    "pipeline_last_success_timestamp_seconds",
    "pipeline_last_reported_timestamp_seconds",
    "pipeline_rejection_rate",
    "pipeline_last_run_duration_seconds",
    "pipeline_db_up",
    "pipeline_exporter_last_refresh_timestamp_seconds",
)

REQUIRED_ALERTS = (
    "PipelineRunFailed",
    "PipelineStale",
    "PipelineNeverReported",
    "HighRejectionRate",
)


def refresh_metrics_from_db(settings: Settings | None = None) -> bool:
    """Refresh every gauge from PostgreSQL.

    Returns:
        ``True`` when the database was reachable, ``False`` otherwise.  A failed
        refresh must never raise: the exporter has to stay alive to report
        ``pipeline_db_up=0``.
    """

    # TODO [OB]: Query etl.load.fetch_metrics_snapshots, clear labelled gauges,
    # publish one series per DAG, then set pipeline_db_up/refresh timestamps.
    raise NotImplementedError("TODO [OB]: Implement this telemetry projection")


def emit_run_metrics(run_id: str | None = None, settings: Settings | None = None) -> bool:
    """DAG-friendly entry point that asks the exporter to refresh its snapshot.

    ``run_id`` is accepted for call-site symmetry and logging; the database is
    always the source of truth.
    """

    # TODO [OB]: Refresh the DB-backed registry without breaking the exporter.
    raise NotImplementedError("TODO [OB]: Implement this telemetry projection")


def render_metrics() -> str:
    """Return the Prometheus text exposition (used by tests and diagnostics)."""

    return generate_latest(REGISTRY).decode("utf-8")


def main() -> None:
    """Run the long-lived exporter endpoint.

    The HTTP server is intentionally usable before the metric projections exist:
    the container stays healthy while [OB] implements ``refresh_metrics_from_db``.
    """

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(settings.metrics_port, registry=REGISTRY)
    logger.info("Prometheus exporter listening on :%d", settings.metrics_port)
    logger.warning("TODO [OB]: database-backed metric refresh is not implemented yet")

    stop_event = threading.Event()

    def _stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    stop_event.wait()


if __name__ == "__main__":  # pragma: no cover - process entry point
    main()
