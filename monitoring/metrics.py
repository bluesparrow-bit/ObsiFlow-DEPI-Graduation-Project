"""ObsiFlow [OB] - DB-backed Prometheus exporter.

Reads observability.pipeline_runs on every scrape and exposes gauges on :9101.
Because values come from PostgreSQL (not in-process counters), a restart of the
exporter or an Airflow worker never resets history.
"""

from __future__ import annotations

import json
import logging
import os
import time

import psycopg2
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily

log = logging.getLogger("obsiflow.exporter")

# ---------------------------------------------------------------------------
# CHECK THESE AGAINST db/init.sql  (the only place names may need editing)
# ---------------------------------------------------------------------------
TABLE = "observability.pipeline_runs"
COL_STATUS = "status"
COL_STARTED = "started_at"
COL_FINISHED = "finished_at"  # may be called ended_at / completed_at
COL_EXTRACTED = "records_extracted"
COL_LOADED = "records_loaded"
COL_REJECTED = "records_rejected"
COL_REJ_RATE = "rejection_rate"
COL_ERRORS = "error_summary"  # per-family failures (JSON expected)
# ---------------------------------------------------------------------------

DAG_ID = os.getenv("OBSIFLOW_DAG_ID", "dq_etl_pipeline")
PORT = int(os.getenv("EXPORTER_PORT", "9101"))
STATUSES = ["SUCCESS", "FAILED", "RUNNING"]
RULE_FAMILIES = ["null", "duplicate", "schema", "row_count", "range", "referential"]


def _connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "obsiflow"),
        user=os.getenv("POSTGRES_USER", "obsiflow"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        connect_timeout=5,
    )


def _family_counts(raw) -> dict[str, float]:
    """Turn error_summary into {family: failures}. Tolerates str/dict/list."""
    counts = {f: 0.0 for f in RULE_FAMILIES}
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(data, dict):
            for family in RULE_FAMILIES:
                val = data.get(family, 0)
                if isinstance(val, (list, dict)):
                    val = len(val)
                counts[family] = float(val or 0)
    except (TypeError, ValueError):
        log.warning("could not parse %s", COL_ERRORS)
    return counts


class PipelineCollector:
    def collect(self):
        up = GaugeMetricFamily("pipeline_db_up", "Exporter can reach PostgreSQL")
        try:
            conn = _connect()
        except Exception:
            log.exception("database unreachable")
            up.add_metric([], 0)
            yield up
            return

        try:
            with conn, conn.cursor() as cur:
                yield from self._collect(cur)
            up.add_metric([], 1)
        except Exception:
            log.exception("query failed")
            up.add_metric([], 0)
        finally:
            conn.close()
        yield up

    def _collect(self, cur):
        # 1) runs by status
        runs = GaugeMetricFamily(
            "pipeline_runs_total", "Runs by status", labels=["dag_id", "status"]
        )
        cur.execute(f"SELECT {COL_STATUS}, COUNT(*) FROM {TABLE} GROUP BY {COL_STATUS}")
        seen = dict(cur.fetchall())
        for status in sorted(set(STATUSES) | set(seen)):
            runs.add_metric([DAG_ID, status], float(seen.get(status, 0)))
        yield runs

        # 2) record counters across all runs
        cur.execute(
            f"SELECT COALESCE(SUM({COL_EXTRACTED}),0), COALESCE(SUM({COL_LOADED}),0),"
            f" COALESCE(SUM({COL_REJECTED}),0) FROM {TABLE}"
        )
        extracted, loaded, rejected = cur.fetchone()
        for name, doc, val in (
            (
                "pipeline_records_processed_total",
                "Extracted rows across runs",
                extracted,
            ),
            ("pipeline_records_loaded_total", "Loaded rows across runs", loaded),
            (
                "pipeline_records_rejected_total",
                "Quarantined rows across runs",
                rejected,
            ),
        ):
            g = GaugeMetricFamily(name, doc)
            g.add_metric([], float(val))
            yield g

        # 3) timestamps
        cur.execute(
            f"SELECT COALESCE(EXTRACT(EPOCH FROM MAX({COL_FINISHED})) FILTER "
            f"(WHERE {COL_STATUS}='SUCCESS'), 0), "
            "COALESCE(EXTRACT(EPOCH FROM MAX("
            f"COALESCE({COL_FINISHED},{COL_STARTED}))), 0) "
            f"FROM {TABLE}"
        )
        last_success, last_reported = cur.fetchone()
        g = GaugeMetricFamily(
            "pipeline_last_success_timestamp_seconds", "Finish time of latest success"
        )
        g.add_metric([], float(last_success))
        yield g
        g = GaugeMetricFamily(
            "pipeline_last_reported_timestamp_seconds", "Time of latest run report"
        )
        g.add_metric([], float(last_reported))
        yield g

        # 4) latest run
        cur.execute(
            f"SELECT {COL_STATUS}, COALESCE({COL_REJ_RATE},0), {COL_ERRORS}, "
            f"COALESCE(EXTRACT(EPOCH FROM ({COL_FINISHED} - {COL_STARTED})), 0) "
            f"FROM {TABLE} ORDER BY {COL_STARTED} DESC LIMIT 1"
        )
        row = cur.fetchone()
        status, rej_rate, errors, duration = row if row else (None, 0, None, 0)

        g = GaugeMetricFamily(
            "pipeline_last_run_status", "Latest run status (one-hot)", labels=["status"]
        )
        for s in STATUSES:
            g.add_metric([s], 1.0 if status == s else 0.0)
        yield g

        g = GaugeMetricFamily("pipeline_rejection_rate", "Latest rejection fraction")
        g.add_metric([], float(rej_rate))
        yield g

        g = GaugeMetricFamily(
            "pipeline_last_run_duration_seconds", "Latest run duration in seconds"
        )
        g.add_metric([], float(duration))
        yield g

        g = GaugeMetricFamily(
            "pipeline_dq_failures_total",
            "Latest-run failures by rule family",
            labels=["rule_family"],
        )
        for family, count in _family_counts(errors).items():
            g.add_metric([family], count)
        yield g


def emit_run_metrics(run_id: str | None = None, *args, **kwargs) -> dict:
    """Hook called by the DAG's ``emit_metrics`` task.

    The exporter is DB-backed: every metric is derived from
    observability.pipeline_runs on scrape, so there is nothing to push here.
    This logs a snapshot of the run so the Airflow task log shows what
    Prometheus will report. It never raises, so telemetry cannot fail a run.
    """
    snapshot: dict = {"run_id": run_id, "status": None, "found": False}
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                if run_id:
                    cur.execute(
                        f"SELECT {COL_STATUS}, {COL_REJ_RATE} FROM {TABLE} "
                        "WHERE run_id = %s",
                        (str(run_id),),
                    )
                else:
                    cur.execute(
                        f"SELECT {COL_STATUS}, {COL_REJ_RATE} FROM {TABLE} "
                        f"ORDER BY {COL_STARTED} DESC LIMIT 1"
                    )
                row = cur.fetchone()
                if row:
                    snapshot.update(
                        found=True, status=row[0], rejection_rate=float(row[1] or 0)
                    )
        finally:
            conn.close()
    except Exception:
        log.exception("emit_run_metrics could not read run state")
    log.info("emit_run_metrics snapshot: %s", snapshot)
    return snapshot


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    REGISTRY.register(PipelineCollector())
    start_http_server(PORT)
    log.info("ObsiFlow exporter listening on :%d", PORT)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
