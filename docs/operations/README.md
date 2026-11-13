# Operations & Observability

This guide outlines the monitoring, operational metrics, and alerting architecture for the ObsiFlow platform.

---

## Observability Architecture

ObsiFlow integrates end-to-end telemetry into every stage of the pipeline:

```text
[ Airflow Pipeline Tasks ]
            │
            ├─► Database Execution Log (PostgreSQL `pipeline_runs`)
            │
            └─► Prometheus Metrics Exporter (HTTP `/metrics`)
                                │
                                ▼
                       [ Prometheus Server ]
                                │
                                ▼
                    [ Grafana Dashboard & Alerts ]
```

---

## Core Operational Metrics

Prometheus tracks the health, volume, and data quality of each pipeline run:

| Metric Name | Type | Description |
|---|---|---|
| `pipeline_run_duration_seconds` | Gauge / Histogram | Execution time of the overall pipeline run and individual stages. |
| `pipeline_runs_total` | Counter | Total number of pipeline executions partitioned by status (`success`, `failed`). |
| `pipeline_records_processed_total` | Counter | Total incoming records processed per batch. |
| `pipeline_records_loaded_total` | Counter | Total valid records successfully committed to target database. |
| `pipeline_records_rejected_total` | Counter | Total invalid records quarantined across all rule families. |
| `pipeline_dq_failures_by_rule_total` | Counter | Count of failures partitioned by rule family (`null`, `duplicate`, `schema_type`, `range`, `referential`). |
| `pipeline_last_success_timestamp_seconds` | Gauge | Unix timestamp of the most recent successful pipeline execution. |

---

## Planned Alert Scenarios

Four automated alert scenarios are defined to guarantee proactive detection before downstream consumption:

1. **Pipeline Run Failure (`Alert: PipelineRunFailed`)**:
   - Triggers immediately when a task fails permanently after retries have exhausted.
2. **Stale Pipeline (`Alert: PipelineStale`)**:
   - Triggers when no successful run has occurred within the expected scheduling interval.
3. **Never-Reported Pipeline (`Alert: PipelineNeverReported`)**:
   - Triggers if expected pipeline telemetry has not been emitted after scheduled startup window.
4. **High Data-Rejection Rate (`Alert: HighRejectionRate`)**:
   - Triggers when the proportion of quarantined records exceeds 10% of total batch records in a single run (`pipeline_records_rejected_total / pipeline_records_processed_total > 0.10`).

---

## Execution Metadata Logging

Every pipeline execution records a traceable audit row in PostgreSQL containing:
- `run_id`: Unique execution identifier
- `dag_id`: Airflow DAG identifier
- `status`: Final run state (`RUNNING`, `SUCCESS`, `FAILED`)
- `start_time` & `end_time`: Execution timestamps
- `duration_seconds`: Total runtime
- `records_extracted`, `records_loaded`, `records_rejected`: Record counts
- `error_summary`: Diagnostic traceback or error details in the event of failure
