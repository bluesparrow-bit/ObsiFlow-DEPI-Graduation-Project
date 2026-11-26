# ObsiFlow: Data Quality & Pipeline Observability Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

</div>

ObsiFlow is a contract-first ETL and data-quality platform. It validates every batch
before loading, quarantines defective rows with complete failure reasons, guarantees
idempotent reruns, and projects pipeline health into Prometheus and Grafana.

The repository is an **interface-first base skeleton**: the shared contracts,
database schema, containers, and CI are complete so six engineering roles can work
in parallel. Role-owned modules are deliberate stubs marked with `TODO [ROLE]` and
`NotImplementedError`; tests that depend on them are skipped until implemented.

---

## Table of Contents

- [What the platform does](#what-the-platform-does)
- [Team onboarding & where to write your code](#team-onboarding--where-to-write-your-code)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Quick start with Docker](#quick-start-with-docker)
- [Local development and tests](#local-development-and-tests)
- [Sample batches and expected outcomes](#sample-batches-and-expected-outcomes)
- [Data contracts and rule families](#data-contracts-and-rule-families)
- [Observability](#observability)
- [CI/CD gates](#cicd-gates)
- [Team and ownership](#team-and-ownership)
- [Documentation](#documentation)

---

## What the platform does

- **Extracts** CSV batches as raw strings and handles missing, empty, unreadable, and
  malformed files without unhandled crashes.
- **Validates** all six DQ rule families: nulls, duplicates, schema/type drift,
  row-count bounds, numeric/date ranges, and referential integrity.
- **Quarantines** every rejected row in `quality.error_records` with raw payload,
  rule families, and failure reasons.
- **Transforms and loads** valid rows with an idempotent PostgreSQL
  `ON CONFLICT (transaction_id) DO UPDATE` upsert.
- **Audits** each run in `observability.pipeline_runs` with counters, duration,
  rejection rate, and per-family failures.
- **Observes** the database through a Prometheus exporter and provisions a Grafana
  dashboard plus four alert rules.
- **Verifies** everything in CI: lint, unit rules, Airflow DagBag integrity,
  PostgreSQL integration, and Docker Compose validity.

## Team onboarding & where to write your code

The shared contract, schema, infrastructure, and CI are already in place. Implement
the stubs below and keep changes inside your role's boundary. Changes to
`etl/contracts.py`, `config/settings.py`, or `db/init.sql` require a coordinated PR
because every role depends on them.

| Role | Owns | Start here |
|---|---|---|
| **`[DE]`** Data Pipeline Engineering | Extraction, transformation, idempotent loading, stage service | `etl/extract.py`, `etl/transform.py`, `etl/load.py`, `etl/pipeline.py` |
| **`[DQ]`** Data Quality | Six rule families, valid/invalid split, quarantine persistence, thresholds | `validation/engine.py`, `validation/quarantine.py`, `validation/rules/*.py`, `config/dq_rules_config.yaml` |
| **`[OR]`** Orchestration & Reliability | DAG wiring, retries, failure callbacks, run audit logging | `dags/dq_etl_pipeline_dag.py` |
| **`[OB]`** Observability | DB-backed exporter, metric projections, four alerts, Grafana provisioning | `monitoring/metrics.py`, `monitoring/prometheus.yml`, `monitoring/alert_rules.yml`, `monitoring/grafana/provisioning/` |
| **`[QA]`** Quality Assurance | Unit, DagBag, integration, and telemetry test suites | `tests/unit/`, `tests/integration/`, `tests/dag_integrity/` |
| **`[DO]`** DevOps | Images, Compose stack, environment templates, CI gates | `Dockerfile`, `docker-compose.yml`, `.github/workflows/ci.yml`, `.env.example`, `.gitignore` |

Search for `TODO [DE]`, `TODO [DQ]`, `TODO [OR]`, `TODO [OB]`, `TODO [QA]`, and
`TODO [DO]` to find every hand-off point. As a role finishes a module, remove the
matching `@pytest.mark.skip(reason="Pending role implementation")` decorators.

## Architecture

```text
[ CSV batch ]
      │
      ▼
[ etl/extract.py ] ──► raw strings, graceful failures
      │
      ▼
[ validation/engine.py ] ── six independent rule families
      │
      ├── invalid ──► [ validation/quarantine.py ] ──► quality.error_records
      │
      └── valid ────► [ etl/transform.py ] ──► [ etl/load.py ]
                                                    │
                                                    ▼
                                          target.transactions
                                                    │
                                                    ▼
                                     observability.pipeline_runs
                                                    │
                                                    ▼
                     monitoring/metrics.py (:9101) ──► Prometheus ──► Grafana
                                                                    └─► alerts
```

The Airflow DAG is deliberately thin:

```text
begin_run → extract → validate → load → emit_metrics → done
```

No pandas, psycopg2, or SQL lives in `dags/`. Permanent validation failures become
`AirflowFailException` (no pointless retries); transient database/network errors use
exponential-backoff retries.

## Repository layout

```text
.
├── .github/workflows/ci.yml        # lint + unit + DagBag + integration + compose
├── config/
│   ├── settings.py                 # Pydantic Settings, validated DQ config
│   └── dq_rules_config.yaml        # thresholds, allowed values, bounds
├── data/
│   ├── sample_valid.csv            # 20 clean rows (0% expected rejection)
│   └── sample_corrupted.csv        # 9 rows covering all six DQ failure types
├── db/init.sql                     # schemas, tables, indexes, reference seed
├── etl/
│   ├── contracts.py                # Pydantic v2 contracts + thread-safe counters
│   ├── extract.py                  # graceful CSV extraction
│   ├── transform.py                # deterministic normalization
│   ├── load.py                     # idempotent upsert + audit queries
│   └── pipeline.py                 # Airflow-independent stage service
├── validation/
│   ├── engine.py                   # aggregates all six families, splits rows
│   ├── quarantine.py               # idempotent dead-letter persistence
│   └── rules/                      # null, duplicate, schema, row-count, range, referential
├── dags/dq_etl_pipeline_dag.py     # thin DAG: wiring, retries, callbacks
├── monitoring/
│   ├── metrics.py                  # DB-backed Prometheus exporter
│   ├── prometheus.yml              # scrape config
│   ├── alert_rules.yml             # four alert rules
│   └── grafana/provisioning/       # datasource + provisioned dashboard
├── tests/
│   ├── unit/test_dq_rules.py       # parametrized pass/fail for every family
│   ├── dag_integrity/              # DagBag, cycles, retries, tags, thinness
│   └── integration/                # ETL flow, reliability, telemetry reconciliation
├── docs/
│   ├── architecture.md             # data flow, contracts, ownership, failure model
│   └── runbook.md                  # alert procedures, recovery, idempotency checks
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml                  # ruff + pytest + coverage config
├── requirements.txt
├── .env.example
└── README.md
```

## Quick start with Docker

```bash
cp .env.example .env
# Edit .env for local values if you wish; the defaults are development-only.

docker compose up --build -d
docker compose ps
```

Services:

| Service | Port | Notes |
|---|---|---|
| Airflow webserver | 8080 | admin / password from `.env` |
| PostgreSQL | 5432 | app schema is created from `db/init.sql` |
| Metrics exporter | 9101 | raw `/metrics` endpoint |
| Prometheus | 9090 | scrapes the exporter every 15s |
| Grafana | 3000 | dashboard is provisioned automatically |

First checks:

```bash
curl -s http://localhost:9101/metrics | grep '^pipeline_' | sort
curl -s http://localhost:9090/-/healthy
```

Trigger a run:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger dq_etl_pipeline \
  --conf '{"source_file":"data/sample_valid.csv"}'
```

Then verify:

```bash
docker compose exec postgres psql -U obsiflow -d obsiflow -c \
  "SELECT run_id, status, records_extracted, records_loaded,
          records_rejected, rejection_rate, error_summary
   FROM observability.pipeline_runs ORDER BY started_at DESC LIMIT 5;"
```

Run the intentionally corrupted batch to exercise all six rule families and the
blocking row-count policy:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger dq_etl_pipeline \
  --conf '{"source_file":"data/sample_corrupted.csv"}'
```

The run is marked `FAILED` (row count below the configured minimum), all 8 bad rows
are quarantined, the valid control row is staged but not loaded, and the target table
remains unchanged.

## Local development and tests

Python 3.11+ is required.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH="$PWD"
```

Start PostgreSQL only:

```bash
docker compose up -d postgres
```

Apply the schema (idempotent):

```bash
docker compose exec -T postgres psql -U obsiflow -d obsiflow -v ON_ERROR_STOP=1 \
  < db/init.sql
```

### Test suites

```bash
# Fast: every rule family and contract, no database or Airflow required
pytest tests/unit -m unit

# Airflow DagBag import errors, cycles, retries, tags, thinness
pytest tests/dag_integrity -m dag_integrity

# Real PostgreSQL: valid/invalid flow, idempotency, quarantine, retries, telemetry
pytest tests/integration -m integration

# Everything with coverage
pytest --cov=config --cov=etl --cov=validation --cov=monitoring --cov-report=term-missing
```

Integration fixtures skip when PostgreSQL is unavailable. Set
`OBSIFLOW_REQUIRE_DB=1` to make an unavailable database a hard failure (CI does this).

### Run the pipeline without Airflow

```bash
python -m etl.pipeline --source data/sample_valid.csv
```

### Lint and format

```bash
ruff check config etl validation dags monitoring tests
ruff format --check config etl validation dags monitoring tests
```

## Sample batches and expected outcomes

| File | Rows | Intended coverage | Expected outcome |
|---|---|---|---|
| `data/sample_valid.csv` | 20 | Clean, all references exist | `SUCCESS`, 20 loaded, 0 rejected |
| `data/sample_corrupted.csv` | 9 | null, duplicate, schema type, range, referential, row-count | `FAILED` (blocking row count), 8 quarantined, 0 loaded |

The corrupted batch contains one valid control row, two null defects, one duplicate,
three schema/type defects, two range defects, and one orphan account. Its 9 rows are
below `min_rows: 10`, which is the sixth family and blocks the whole load.

The deterministic generator in `data/genration_script/` produces larger banking
batches (including high-rejection, schema-corrupt, empty, and unreadable variants):

```bash
python data/genration_script/generate_data.py --out /tmp/obsiflow-data --rows 1000 --seed 42
```

## Data contracts and rule families

`TransactionRecord` enforces:

| Field | Rule |
|---|---|
| `transaction_id`, `account_id`, `txn_type`, `channel`, `status` | non-blank strings |
| `amount` | finite `Decimal`, no commas or currency symbols |
| `currency` | exactly three characters |
| `txn_date` | ISO-8601 `YYYY-MM-DD` |

| # | Family | Default policy |
|---|---|---|
| 1 | `null` | mandatory blank → quarantine row |
| 2 | `duplicate` | keep first business key, quarantine later rows |
| 3 | `schema` | missing columns block batch; type/enum drift quarantines row |
| 4 | `row_count` | outside `[10, 100000]` blocks batch |
| 5 | `range` | amount outside `[0.01, 1000000]` or date outside `[2020-01-01, 2026-11-05]` → quarantine |
| 6 | `referential` | `account_id` absent from `reference.accounts` → quarantine |

Thresholds live in `config/dq_rules_config.yaml` and are validated at startup.
The 10% rejection rate is an **alert**, not a load gate; structural and row-count
anomalies fail closed.

## Observability

The exporter (`monitoring/metrics.py`) reads PostgreSQL and exposes gauges, so worker
restarts cannot reset history. Key metrics:

| Metric | Meaning |
|---|---|
| `pipeline_runs_total{dag_id,status}` | runs by status |
| `pipeline_records_processed_total` | extracted rows across runs |
| `pipeline_records_loaded_total` | loaded rows across runs |
| `pipeline_records_rejected_total` | quarantined rows across runs |
| `pipeline_dq_failures_total{rule_family}` | latest-run failures by family |
| `pipeline_last_run_status{status}` | latest run one-hot status |
| `pipeline_last_success_timestamp_seconds` | latest successful finish |
| `pipeline_last_reported_timestamp_seconds` | latest run report |
| `pipeline_rejection_rate` | latest rejection fraction |
| `pipeline_last_run_duration_seconds` | latest run duration |
| `pipeline_db_up` | exporter database reachability |

Four alerts are provisioned in `monitoring/alert_rules.yml`:

1. `PipelineRunFailed`
2. `PipelineStale` (> 26 hours without success)
3. `PipelineNeverReported`
4. `HighRejectionRate` (> 10%)

Grafana provisions the `obsiflow-prometheus` datasource and the
**ObsiFlow — Data Quality & Pipeline Health** dashboard.

## CI/CD gates

`.github/workflows/ci.yml` runs on every push and pull request:

1. **Lint** — `ruff check` and `ruff format --check`.
2. **Unit** — parametrized rule/contract tests with coverage.
3. **DagBag** — import errors, task graph, retries, tags, and thin-DAG checks.
4. **Integration** — PostgreSQL service, schema bootstrap, ETL/idempotency/quarantine,
   failure-policy, and telemetry reconciliation tests.
5. **Compose** — `docker compose config` plus required-service assertions.
6. **CI gate** — fails the workflow unless all five jobs succeed.

## Team and ownership

| Member | Role | Area |
|---|---|---|
| **Mohammad Maher Metwaly** | Data Quality / DevOps (Lead) | `DQ` / `DO` |
| **Hager Abedlnasser Seddiq** | DevOps / Data Quality | `DO` / `DQ` |
| **Elhussieny Sabry Elshahat** | Reliability / Orchestration | `QA` / `OR` |
| **Sara Ahmed Abdelhamid** | Orchestration / Reliability | `OR` / `QA` |
| **Soltan Akram Soltan** | Data Pipeline Engineering | `DE` |
| **Malak Magdy Mohamed** | Observability Engineering | `OB` |

Ownership boundaries and parallel-development rules are documented in
[docs/architecture.md](docs/architecture.md#3-module-ownership-and-boundaries).

## Documentation

- [Architecture and contracts](docs/architecture.md)
- [Operations runbook](docs/runbook.md)
- [Documentation index](docs/README.md)
- [Contributing guide](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

## Security

- Never commit `.env`, credentials, tokens, or database dumps.
- Use `.env.example` as the template and inject real secrets in your environment.
- The Compose defaults are development-only; production must use managed secrets,
  a separate Airflow metadata database, TLS, and least-privilege database roles.
## Observability (Role 4)

The platform exposes pipeline health through Prometheus and Grafana.

- **Metrics exporter** (`monitoring/metrics.py`): reads run history from PostgreSQL and exposes metrics on port 9101 (`/metrics`): run duration, success/failed runs, processed/loaded/rejected records, and data-quality failures per rule family.
- **Prometheus** (`monitoring/prometheus.yml`): scrapes the exporter every 15 seconds.
- **Alerts** (`monitoring/alert_rules.yml`): alert rules for pipeline failures and abnormal runs.
- **Grafana dashboard** (`monitoring/grafana/provisioning`): auto-loaded dashboard showing duration, run status, record counts, and DQ errors.

### How to run
    docker compose up -d --build

| Service | URL |
|---|---|
| Metrics | http://localhost:9101/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |
