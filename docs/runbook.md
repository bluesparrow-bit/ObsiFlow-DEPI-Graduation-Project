# ObsiFlow Runbook

Audience: on-call `OR`, `DQ`, and `OB` engineers. All commands assume the repository
root and the Docker Compose stack. Replace `obsiflow`/`admin` with your local values
from `.env`; never paste real credentials into tickets or chat.

## 1. First five minutes

```bash
docker compose ps
docker compose logs --tail=200 airflow-scheduler
docker compose logs --tail=200 metrics-exporter
docker compose exec postgres psql -U obsiflow -d obsiflow -c \
  "SELECT run_id, status, started_at, finished_at, records_extracted,
          records_loaded, records_rejected, rejection_rate, error_summary
   FROM observability.pipeline_runs
   ORDER BY started_at DESC LIMIT 5;"
```

Check the four signals:

1. Is the latest run `FAILED`? → `PipelineRunFailed` procedure.
2. Is the latest successful run older than 26h? → `PipelineStale`.
3. Has telemetry ever reported? → `PipelineNeverReported`.
4. Is the latest rejection rate above 10%? → `HighRejectionRate`.

Useful URLs (local defaults):

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Prometheus alerts | http://localhost:9090/alerts |
| Grafana | http://localhost:3000 (dashboard: ObsiFlow — Data Quality & Pipeline Health) |
| Raw metrics | http://localhost:9101/metrics |

## 2. Alert response procedures

### PipelineRunFailed

**Meaning:** the most recent pipeline run reached `FAILED`.

Transient database/network failures stay `RUNNING` while Airflow retries remain and
are marked `FAILED` only when retries are exhausted, so a brief outage does not raise
this alert prematurely.

1. Get the failure summary and counters:
   ```sql
   SELECT run_id, source_file, error_summary, records_extracted,
          records_valid, records_loaded, records_rejected, failures_by_rule
   FROM observability.pipeline_runs
   WHERE status = 'FAILED'
   ORDER BY started_at DESC
   LIMIT 1;
   ```
2. Classify the failure:
   - `Source file does not exist` / `not valid UTF-8` / `could not be parsed`:
     upstream delivery problem. Restore the file, then use a **new** run id.
   - `Missing required column(s)`: schema drift. Fix the producer or the rule
     allow-list, then replay. This is non-retryable by design.
   - `Batch has N rows; minimum/maximum`: truncated/oversized batch. Confirm the
     producer's row count before replaying. Never lower `min_rows` just to pass.
   - `Foreign key ... does not exist` on load: reference data is behind the batch.
     Refresh `reference.accounts` before replaying.
   - `Transient PostgreSQL failure`: check database health; Airflow retries
     transient failures automatically. Clear the task to retry after recovery.
3. Validate data quality after the fix:
   ```sql
   SELECT rule_families, count(*)
   FROM quality.error_records
   WHERE run_id = '<failed-run-id>'
   GROUP BY rule_families
   ORDER BY count(*) DESC;
   ```
4. Replay with a new run id (safe because loads are upserts):
   ```bash
   docker compose exec airflow-scheduler \
     airflow dags trigger dq_etl_pipeline \
     --conf '{"source_file":"data/sample_valid.csv"}'
   ```
5. Confirm `status='SUCCESS'` and inspect `records_loaded`, `records_rejected`,
   `rejection_rate`, and `error_summary IS NULL`.

**Do not** repeatedly clear a permanent failure. It will burn retries and hide the
root cause; `AirflowFailException` exists to prevent exactly that.

### PipelineStale

**Meaning:** no `SUCCESS` run in more than 26 hours (daily schedule + 2h grace).

1. Check the scheduler and webserver:
   ```bash
   docker compose ps airflow-scheduler airflow-webserver
   docker compose logs --tail=200 airflow-scheduler
   ```
2. Check whether the DAG is paused:
   ```bash
   docker compose exec airflow-scheduler airflow dags list-runs -d dq_etl_pipeline
   ```
   Unpause in the UI or with `airflow dags unpause dq_etl_pipeline`.
3. If Airflow metadata is unavailable, verify PostgreSQL and the metadata tables.
4. If the scheduler is healthy but the DAG never runs, check `start_date`,
   `schedule`, timezone, and that the file exists in `dags/`.
5. Trigger a manual run, then confirm `pipeline_last_success_timestamp_seconds`
   advances within one exporter refresh interval (default 15s).

### PipelineNeverReported

**Meaning:** `pipeline_last_reported_timestamp_seconds` is absent or zero.

1. Check `pipeline_db_up` at http://localhost:9101/metrics. `0` means the exporter
   cannot read PostgreSQL.
2. Check the exporter:
   ```bash
   docker compose ps metrics-exporter
   docker compose logs --tail=200 metrics-exporter
   curl -s http://localhost:9101/metrics | grep -E 'pipeline_(db_up|last_reported)'
   ```
3. Check the audit table:
   ```sql
   SELECT count(*), max(started_at) FROM observability.pipeline_runs;
   ```
   Zero rows means the DAG has never executed. Trigger it or unpause it.
4. Check Prometheus target health at http://localhost:9090/targets; the
   `obsiflow-telemetry` target must be `UP`.
5. Restart the exporter only after confirming the database is healthy:
   ```bash
   docker compose restart metrics-exporter
   ```

### HighRejectionRate

**Meaning:** the latest run rejected more than 10% of extracted rows. The load may
still have succeeded with the remaining valid rows.

1. Quantify by family:
   ```sql
   SELECT rule_families, count(*) AS rows
   FROM quality.error_records
   WHERE run_id = '<latest-run-id>'
   GROUP BY rule_families
   ORDER BY rows DESC;
   ```
2. Inspect representative failures:
   ```sql
   SELECT row_number, transaction_id, rule_families, failure_reasons, raw_record
   FROM quality.error_records
   WHERE run_id = '<latest-run-id>'
   ORDER BY row_number
   LIMIT 20;
   ```
3. Decide ownership:
   - One family dominating and source values clearly malformed → upstream producer
     issue; request a corrected batch.
   - `referential` spike → refresh `reference.accounts` before replay.
   - `null`/`schema` spike after a release → schema drift; compare source headers to
     `config/dq_rules_config.yaml`.
   - Threshold too strict for a legitimate business change → change the YAML in a
     reviewed PR; never edit live thresholds ad hoc.
4. Replay the corrected batch with a new run id and verify the rate drops below 10%.

## 3. Quarantine triage and replay

Rejected rows are never discarded. A row is written once per `(run_id, row_number)`
with all failures in `failure_reasons`.

```sql
-- Failure volume by family for a run
SELECT rule_families, count(*) FROM quality.error_records
WHERE run_id = '<run-id>' GROUP BY rule_families ORDER BY 2 DESC;

-- Raw payload for a specific source row
SELECT raw_record, failure_reasons FROM quality.error_records
WHERE run_id = '<run-id>' AND row_number = 42;

-- Families observed across the last day
SELECT unnest(rule_families) AS family, count(*)
FROM quality.error_records
WHERE created_at > now() - interval '1 day'
GROUP BY family ORDER BY 2 DESC;
```

After correcting the source file, rerun with a **new** run id. Do not delete
quarantine rows for an audit trail; retention/archival is a separate reviewed job.

## 4. Idempotency and backfill verification

The target upsert is keyed on `transaction_id`; the quarantine upsert is keyed on
`(run_id, row_number)`; the run row is keyed on `run_id`.

```sql
-- Must return zero rows
SELECT transaction_id, count(*) FROM target.transactions
GROUP BY transaction_id HAVING count(*) > 1;

-- Must not exceed the number of distinct valid records across replays
SELECT count(*) FROM target.transactions;

-- One row per source row per run
SELECT run_id, row_number, count(*) FROM quality.error_records
GROUP BY run_id, row_number HAVING count(*) > 1;
```

Replaying a batch is safe. Reusing the same Airflow `run_id` restarts the audit row
and re-upserts quarantine/target data; using a new run id gives a cleaner audit trail
and is preferred for operational replays.

## 5. Database operations

Apply the idempotent schema to an existing volume:

```bash
docker compose exec -T postgres psql -U obsiflow -d obsiflow -v ON_ERROR_STOP=1 \
  < db/init.sql
```

Inspect the latest run per DAG:

```sql
SELECT * FROM observability.v_latest_runs ORDER BY dag_id;
```

Check table sizes and quarantine growth:

```sql
SELECT relname, n_live_tup
FROM pg_stat_user_tables
WHERE schemaname IN ('reference', 'target', 'quality', 'observability')
ORDER BY n_live_tup DESC;
```

If Airflow metadata is corrupted in local development, stop the stack and reset only
the Airflow tables/database after backing up anything you need. Never drop the
`reference`, `target`, `quality`, or `observability` schemas as a routine fix.

## 6. Telemetry verification

```bash
curl -s http://localhost:9101/metrics | grep '^pipeline_' | sort
```

Expected after at least one run:

- `pipeline_db_up 1`
- `pipeline_runs_total{...status="success"}` matches the count in `pipeline_runs`
- `pipeline_records_processed_total` equals `SUM(records_extracted)`
- `pipeline_last_success_timestamp_seconds` equals the latest successful
  `finished_at` epoch
- `pipeline_dq_failures_total` equals the latest run's `failures_by_rule`

Prometheus scrapes every 15s. If a gauge looks stale, check the exporter logs and
the last refresh timestamp before touching the pipeline.

## 7. Manual execution without Airflow

For debugging or a controlled backfill:

```bash
python -m etl.pipeline --source data/sample_valid.csv --no-metrics
python -m etl.pipeline --source data/sample_corrupted.csv --no-metrics
```

The CLI uses the same stages and persistence as the DAG. It exits non-zero on a
permanent/transient pipeline error and always attempts to finalize the run.

## 8. Orchestration retry and failure policy

Owner: `OR`. Task chain: `begin_run >> extract >> validate >> load >> emit_metrics >> done`.

| Failure class | Example | Airflow behaviour | `pipeline_runs.status` |
|---|---|---|---|
| Transient (`TransientPipelineError`) | PostgreSQL restart, connection reset | Retried up to 3 times: 2 min, 4 min, 8 min (exponential backoff, capped at 30 min) | Stays `RUNNING` while retries remain, then `FAILED` |
| Permanent (`PermanentPipelineError`) | Missing column, row count out of bounds, missing source file | Raised as `AirflowFailException`: no retries | `FAILED` immediately, with `error_summary` |
| Unclassified exception | Unexpected bug | Retried like a transient error (safe default) | `FAILED` once retries are exhausted |
| Metric refresh failure (`emit_metrics`) | Exporter unreachable | Logged as a warning; never fails the run | Unchanged (`SUCCESS`) |

Run audit logging:

- `begin_run` inserts the `RUNNING` row (`etl.pipeline.begin_run_stage`).
- `load` marks the run `SUCCESS` after the upsert (`finalize_run_stage`).
- The DAG failure callback marks the run `FAILED` (`mark_run_failed_stage`). If the
  database is down at that moment the callback logs the problem instead of raising,
  so the original root cause stays visible in the task log.

Each task has a 30-minute `execution_timeout`; the whole run has a 2-hour
`dagrun_timeout`. Only small JSON metadata (`run_id`, `source_file`, counts) crosses
XCom; valid rows are staged under `data/staging/<run_id>/`.

## 9. Ownership and escalation

| Area | Primary | Escalation |
|---|---|---|
| Rule thresholds, quarantine triage | `DQ` | `DE` for contract changes |
| Airflow scheduling, retries, reruns | `OR` | `DO` for infrastructure |
| Prometheus/Grafana, alert noise | `OB` | `DO` for stack availability |
| Database schema, load failures | `DE` | `DO` for PostgreSQL |
| CI failures, images, secrets | `DO` | Team lead |
| Test gaps and release sign-off | `QA` | Team lead |

Severity guidance:

- **Critical** — failed production run, never-reported telemetry, or data corruption
  risk. Acknowledge within 15 minutes; mitigate before root-cause analysis.
- **Warning** — stale schedule or high rejection. Acknowledge within one business
  hour; fix before the next scheduled run.
