# ObsiFlow Architecture

## 1. Purpose and design principles

ObsiFlow is a contract-first ETL and data-quality platform. A batch is extracted as
raw strings, validated by six independent rule families, split into valid and
quarantined records, transformed, and loaded idempotently. Every stage writes
enough state to be retried safely, and every terminal state is projected into
Prometheus from PostgreSQL.

Design principles:

1. **One source of truth per concern** — contracts in `etl/contracts.py`, thresholds
   in `config/dq_rules_config.yaml`, telemetry in `observability.pipeline_runs`.
2. **Business logic never lives in Airflow** — DAGs contain only context wiring.
3. **Fail closed on schema/row-count drift** — a structurally invalid batch cannot
   partially load.
4. **Quarantine, don't discard** — every rejected row keeps its raw payload and all
   failure reasons.
5. **Retries are safe by construction** — upsert keys and quarantine uniqueness make
   at-least-once execution converge to exactly-once state.
6. **Telemetry is derived, not accumulated** — exporter gauges are projected from
   the audit table, so worker restarts cannot reset history.

## 2. Component and data-flow diagram

```text
                    ┌──────────────────────────────────────────────┐
                    │ config/dq_rules_config.yaml                  │
                    │ thresholds + allowed values (validated)      │
                    └──────────────────────┬───────────────────────┘
                                           │
 [CSV batch]                               ▼
      │                        ┌─────────────────────────┐
      └──► etl/extract.py ────►│ validation/engine.py    │
           raw strings         │ 6 rule families         │
           graceful errors     └───────┬─────────┬───────┘
                                       │         │
                            valid rows │         │ invalid rows
                                       ▼         ▼
                         etl/transform.py   validation/quarantine.py
                                       │         │
                                       ▼         ▼
                    etl/load.py (upsert)   quality.error_records
                                       │
                                       ▼
                          target.transactions
                                       │
                                       ▼
                    observability.pipeline_runs
                                       │
                                       ▼
               monitoring/metrics.py (HTTP :9101) ──► Prometheus ──► Grafana
                                                              └──► alert_rules.yml
```

## 3. Module ownership and boundaries

| Path | Owner | Responsibility | Must not contain |
|---|---|---|---|
| `config/` | `DO` + `DQ` | Env settings, validated YAML thresholds | Business transforms |
| `etl/contracts.py` | `DE` + `DQ` | Pydantic contracts, enums, run counters | I/O |
| `etl/extract.py` | `DE` | CSV extraction and graceful failure handling | Validation policy |
| `etl/transform.py` | `DE` | Deterministic normalization and quantization | SQL |
| `etl/load.py` | `DE` | PostgreSQL transactions, upserts, audit reads | Data-quality decisions |
| `validation/rules/` | `DQ` | One module per rule family (bounds shared by row-count + range) | Database access |
| `validation/engine.py` | `DQ` | Aggregation and valid/invalid split | Persistence |
| `validation/quarantine.py` | `DQ` | Idempotent dead-letter writes | Rule logic |
| `etl/pipeline.py` | `DE` + `OR` | Stage service, staging artefacts, CLI | Airflow imports |
| `dags/` | `OR` | Thin DAG, retries, callbacks, XCom wiring | pandas, psycopg2, SQL |
| `monitoring/` | `OB` | Exporter, Prometheus, alerts, Grafana | ETL transforms |
| `db/init.sql` | `DE` + `DO` | Schemas, constraints, indexes, seed reference data | Application logic |
| `tests/` | `QA` | Rule, DAG, flow, reliability, telemetry tests | Production code |
| `.github/workflows/` | `DO` | Lint, unit, DagBag, integration, compose gates | Secrets |

## 4. Data contracts

`TransactionRecord` is the canonical row contract. Parsing is intentionally strict:

| Field | Type | Contract rule |
|---|---|---|
| `transaction_id` | string | non-blank, max 64 chars |
| `account_id` | string | non-blank, max 32 chars |
| `txn_type` | string | non-blank; enum checked by schema rule |
| `amount` | `Decimal` | finite, plain decimal only (no commas/symbols) |
| `currency` | string | exactly 3 chars; enum checked by schema rule |
| `txn_date` | `date` | ISO-8601 `YYYY-MM-DD` only |
| `channel` | string | non-blank; enum checked by schema rule |
| `status` | string | non-blank; enum checked by schema rule |

`BatchValidationResult` returns `valid_records` (typed contracts), `rejected_rows`
(raw payload plus all violations), `batch_violations`, `failures_by_family`,
`rejection_rate`, and blocking/alert flags.

`RunCounters` is a thread-safe counter set owned by the pipeline service. It is the
only place counts are accumulated; persistence reads snapshots from it.

## 5. Six rule families and failure policy

| # | Family | Module | Scope | Load impact |
|---|---|---|---|---|
| 1 | `null` | `null_check.py` | Mandatory fields are null/blank | Row quarantined |
| 2 | `duplicate` | `duplicate_check.py` | Duplicate business key in batch | Later rows quarantined (`keep_first`) |
| 3 | `schema` | `schema_check.py` | Missing columns, type drift, enums | Missing columns block batch; bad rows quarantine |
| 4 | `row_count` | `range_check.py` | Batch below/above configured bounds | Blocks batch (fail closed) |
| 5 | `range` | `range_check.py` | Amount/date inclusive bounds | Row quarantined |
| 6 | `referential` | `referential_check.py` | `account_id` absent from `reference.accounts` | Row quarantined |

The rejection-rate threshold (default 10%) is **alert-only**. It never silences or
blocks a load by itself; blocking is reserved for structural or volume anomalies.

### Failure classes

- `PermanentPipelineError`: missing file, unparseable bytes, missing columns, row-count
  violation, contract drift. The DAG converts this to `AirflowFailException` so
  Airflow does not retry a doomed batch.
- `TransientPipelineError`: connection refused/reset, database restart, temporary
  network faults. It propagates and uses the DAG's exponential-backoff retries; the
  failure callback leaves the run `RUNNING` while attempts remain and marks it
  `FAILED` only on the final attempt.

## 6. Persistence model

| Object | Purpose | Key/idempotency |
|---|---|---|
| `reference.accounts` | Foreign-key authority | `account_id` primary key |
| `target.transactions` | Curated warehouse | `transaction_id`; `ON CONFLICT DO UPDATE` |
| `quality.error_records` | Dead-letter store | Unique `(run_id, row_number)`; upsert on retry |
| `observability.pipeline_runs` | Run audit + telemetry source | `run_id` primary key |

`quality.error_records.rule_families` is a `TEXT[]` with a check constraint against
the six family names. `failure_reasons` stores a JSON array with `family`, `field`,
and `message`. `raw_record` preserves the source row.

`observability.v_latest_runs` exposes the most recent run per DAG for operations.

## 7. Orchestration sequence

```text
begin_run
  └─ INSERT pipeline_runs (RUNNING)
extract_and_validate
  ├─ extract CSV as strings
  ├─ load reference account ids
  ├─ run six rule families
  ├─ upsert quality.error_records
  ├─ write data/staging/<run>/valid_records.json + run_summary.json
  ├─ UPDATE pipeline_runs counters/failures
  └─ raise PermanentPipelineError if blocking
transform_and_load
  ├─ read staged contracts
  ├─ normalize/quantize
  └─ upsert target.transactions
finalize_run
  └─ UPDATE status=SUCCESS, finished_at, duration
emit_metrics
  └─ best-effort in-task refresh; the standalone exporter polls PostgreSQL
     on its own interval and is the serving process
```

Failure callback finalizes the run as `FAILED` with a truncated error summary; a
transient failure is not finalized while Airflow still has retry attempts left.
Successful finalization clears any stale error summary. If `begin_run` itself
failed, no run id exists and the callback logs and exits.

## 8. Observability contract

All metrics are gauges projected from PostgreSQL, exposed by
`monitoring/metrics.py` on `OBSIFLOW_METRICS_PORT` (default `9101`):

| Metric | Meaning |
|---|---|
| `pipeline_runs_total{dag_id,status}` | Count of runs per status |
| `pipeline_runs_in_flight{dag_id}` | RUNNING runs |
| `pipeline_records_processed_total{dag_id}` | Sum of extracted rows |
| `pipeline_records_loaded_total{dag_id}` | Sum of loaded rows |
| `pipeline_records_rejected_total{dag_id}` | Sum of quarantined rows |
| `pipeline_dq_failures_total{dag_id,rule_family}` | Latest-run failures by family |
| `pipeline_last_run_status{dag_id,status}` | One-hot latest status |
| `pipeline_last_success_timestamp_seconds{dag_id}` | Latest successful finish |
| `pipeline_last_reported_timestamp_seconds{dag_id}` | Latest run report |
| `pipeline_rejection_rate{dag_id}` | Latest run rejection fraction |
| `pipeline_last_run_duration_seconds{dag_id}` | Latest run duration |
| `pipeline_db_up` | Exporter can read PostgreSQL |
| `pipeline_exporter_last_refresh_timestamp_seconds` | Last successful scrape refresh |

The four alert rules are:

1. `PipelineRunFailed` — latest status is FAILED.
2. `PipelineStale` — no success in more than 26 hours.
3. `PipelineNeverReported` — last-reported metric absent or zero.
4. `HighRejectionRate` — latest rejection fraction above 10%.

## 9. Deployment topology

`docker-compose.yml` starts PostgreSQL, Airflow init/webserver/scheduler, the metrics
exporter, Prometheus, and Grafana. Airflow's metadata tables live in the same local
PostgreSQL database as the application in this development stack; for production,
use a separate metadata database and managed secrets (for example Vault or cloud
secret manager). The app never relies on Airflow's metadata for telemetry.

The custom `Dockerfile` extends `apache/airflow:2.8.4-python3.11`, installs project
requirements, and copies the project to `/opt/airflow/project` with `PYTHONPATH` set.

## 10. Test and CI architecture

| Gate | Command | Proves |
|---|---|---|
| Lint | `ruff check` / `ruff format --check` | Style and import hygiene |
| Unit | `pytest tests/unit -m unit` | Every rule family passes/fails as specified |
| DagBag | `pytest tests/dag_integrity -m dag_integrity` | DAG imports, no cycles, retries, tags, thinness |
| Integration | `pytest tests/integration -m integration` | Valid/invalid flow, idempotency, quarantine, retries, telemetry |
| Compose | `docker compose config --quiet` | Full stack is syntactically valid and complete |

Integration tests require PostgreSQL and apply `db/init.sql`. In CI,
`OBSIFLOW_REQUIRE_DB=1` turns an unavailable database into a hard failure. Locally,
the fixture skips when PostgreSQL is unavailable so unit development remains fast.

## 11. Parallel-development contract

- `DE` can evolve `etl/` against `TransactionRecord` and `RunCounters` without touching DQ rules.
- `DQ` can add a rule by returning `Violation`/`BatchViolation` objects and registering it in `validation/engine.py`.
- `OR` changes DAG task wiring or retry policy without editing business logic.
- `OB` changes metric projections without changing the pipeline, as long as `pipeline_runs` columns remain stable.
- `QA` writes tests against public function signatures and the four database objects.
- `DO` changes images/compose/CI without changing module APIs.

Any contract change (a field, enum, table column, or metric name) requires a coordinated
PR that updates this document, `docs/runbook.md`, tests, and the relevant SQL DDL.
