"""Telemetry reconciliation: Prometheus gauges must equal PostgreSQL state."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from prometheus_client.parser import text_string_to_metric_families
from psycopg2.extras import RealDictCursor

from config.settings import Settings
from etl.contracts import TransientPipelineError
from etl.pipeline import run_pipeline
from monitoring.metrics import refresh_metrics_from_db, render_metrics

pytestmark = [pytest.mark.integration, pytest.mark.skip(reason="Pending role implementation")]


def _metric_value(name: str, **labels: str) -> float | None:
    for family in text_string_to_metric_families(render_metrics()):
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(key) == value for key, value in labels.items()):
                return float(sample.value)
    return None


def _write_mixed_batch(path: Path) -> Path:
    columns = [
        "transaction_id",
        "account_id",
        "txn_type",
        "amount",
        "currency",
        "txn_date",
        "channel",
        "status",
    ]
    rows: list[dict[str, str]] = []
    for index in range(1, 11):
        rows.append(
            {
                "transaction_id": f"TXN6{index:07d}",
                "account_id": f"ACC{index:06d}",
                "txn_type": "DEPOSIT",
                "amount": "100.00",
                "currency": "USD",
                "txn_date": "2025-01-15",
                "channel": "ONLINE",
                "status": "POSTED",
            }
        )
    rows.append({**rows[0], "amount": "222.00"})
    rows.append({**rows[1], "transaction_id": "TXN699999999", "account_id": "ACC999999"})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _db_totals(connection: Any, run_id: str) -> dict[str, Any]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT status, records_extracted, records_loaded, records_rejected,
                   rejection_rate, failures_by_rule, finished_at, duration_seconds
            FROM observability.pipeline_runs
            WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cursor.fetchone()
    connection.commit()
    assert row is not None
    return dict(row)


def test_metrics_reconcile_with_pipeline_runs(
    clean_db: Any,
    settings: Settings,
    tmp_path: Path,
) -> None:
    source = _write_mixed_batch(tmp_path / "telemetry_mixed.csv")
    run = run_pipeline(source, run_id="it-metrics-1", settings=settings, emit_metrics=False)
    db = _db_totals(clean_db, "it-metrics-1")

    assert refresh_metrics_from_db(settings) is True

    dag_id = run.dag_id
    assert _metric_value("pipeline_runs_total", dag_id=dag_id, status="success") == 1
    assert _metric_value("pipeline_runs_total", dag_id=dag_id, status="failed") == 0
    assert _metric_value("pipeline_records_processed_total", dag_id=dag_id) == 12
    assert _metric_value("pipeline_records_loaded_total", dag_id=dag_id) == 10
    assert _metric_value("pipeline_records_rejected_total", dag_id=dag_id) == 2
    assert _metric_value("pipeline_rejection_rate", dag_id=dag_id) == pytest.approx(
        float(db["rejection_rate"]), rel=1e-6
    )
    assert _metric_value("pipeline_dq_failures_total", dag_id=dag_id, rule_family="duplicate") == 1
    assert (
        _metric_value("pipeline_dq_failures_total", dag_id=dag_id, rule_family="referential") == 1
    )
    assert _metric_value("pipeline_dq_failures_total", dag_id=dag_id, rule_family="null") == 0
    assert _metric_value("pipeline_last_run_status", dag_id=dag_id, status="success") == 1
    assert _metric_value("pipeline_last_run_status", dag_id=dag_id, status="failed") == 0
    assert _metric_value("pipeline_db_up") == 1

    last_success = _metric_value("pipeline_last_success_timestamp_seconds", dag_id=dag_id)
    assert last_success == pytest.approx(db["finished_at"].timestamp(), abs=1.0)
    assert _metric_value("pipeline_last_run_duration_seconds", dag_id=dag_id) == pytest.approx(
        float(db["duration_seconds"]), abs=1.0
    )


def test_exporter_survives_database_outage(clean_db: Any, settings: Settings) -> None:
    with patch(
        "monitoring.metrics.fetch_metrics_snapshots",
        side_effect=TransientPipelineError("database down"),
    ):
        assert refresh_metrics_from_db(settings) is False

    assert _metric_value("pipeline_db_up") == 0


def test_never_reported_metric_is_absent_when_no_runs(clean_db: Any, settings: Settings) -> None:
    assert refresh_metrics_from_db(settings) is True
    assert _metric_value("pipeline_db_up") == 1
    assert _metric_value("pipeline_last_reported_timestamp_seconds") is None
