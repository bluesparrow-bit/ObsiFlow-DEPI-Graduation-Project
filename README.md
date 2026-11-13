# ObsiFlow: Data Quality & Pipeline Observability Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

</div>

A production-grade ETL and observability platform that validates data before loading, isolates defective records, exposes comprehensive operational telemetry via Prometheus, and provides proactive alerting in Grafana.

---

## Table of Contents

- [Project Status](#project-status)
- [Problem Statement](#problem-statement)
- [Key Capabilities](#key-capabilities)
- [Architecture & Data Flow](#architecture--data-flow)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Documentation Links](#documentation-links)
- [Team Members & Roles](#team-members--roles)

---

## Project Status

**Current Phase**: Active Development — Foundation, Test Data Generation & Governance (Week 1–2).  
Core baseline specifications, data generators, documentation structure, and contribution governance are established. Implementation of pipeline modules, Airflow orchestration, and Prometheus telemetry is underway.

---

## Problem Statement

Many organizations operate automated data pipelines where data corruption, schema drift, and silent ingestion failures are only detected downstream when business stakeholders report incorrect dashboards. By that time, damaged data has propagated, causing costly backfills and manual root-cause investigations.

ObsiFlow solves this by implementing pre-load validation gates, quarantining defective rows into a dead-letter store, enforcing idempotent re-runs, and capturing real-time pipeline telemetry to detect anomalies before data reaches production tables.

---

## Key Capabilities

- **Six Data Quality Rule Families**: Validates nulls, duplicates, schema/data types, row counts, referential integrity, and range bounds.
- **Dead-Letter Quarantine**: Routes invalid records to a quarantine store with failure reasons, batch IDs, and audit timestamps.
- **Idempotent Loading**: Guarantees safe batch re-runs in PostgreSQL without duplicating records.
- **Airflow Orchestration**: Robust task execution with automated retries for transient issues and explicit failure reporting.
- **Operational Logging**: Persists comprehensive execution metadata (`run_id`, duration, extracted/loaded/rejected counts).
- **Proactive Observability**: Prometheus metrics export and Grafana dashboards tracking health, latency, and four alert scenarios (stale, failed, never-reported, and high-rejection runs).

---

## Architecture & Data Flow

```text
[ Source Files / Batches ]
           │
           ▼
[ 1. Ingestion / Extract ]
           │
           ▼
[ 2. Data Quality Engine ] ──── (Invalid Records) ───► [ Quarantine / Dead-Letter Store ]
           │
     (Valid Records)
           │
           ▼
[ 3. Transformation ]
           │
           ▼
[ 4. Target Database Loading ] (Idempotent Upsert)
           │
           ▼
[ 5. Execution Logging & Metrics ] ───► [ PostgreSQL Logs ] + [ Prometheus Metrics ]
                                                                     │
                                                                     ▼
                                                              [ Grafana Dashboard & Alerts ]
```

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language** | Python 3.10+ | Core ETL modules and data validation logic |
| **Orchestration** | Apache Airflow | Workflow scheduling, task dependency, and retry policies |
| **Data Processing** | Pandas | Ingestion, cleaning, transformation, and validation rules |
| **Database** | PostgreSQL | Target warehouse and pipeline execution audit logs |
| **Metrics** | Prometheus | Scrapes and stores operational telemetry and DQ counters |
| **Visualization & Alerting** | Grafana | Dashboards for pipeline health, run latency, and alerts |
| **Containerization** | Docker & Docker Compose | Multi-container local execution environment |
| **Version Control** | Git & GitHub | Source control, collaborative code reviews, and governance |

---

## Repository Structure

```text
.
├── .editorconfig              # Consistent multi-editor formatting standards
├── .gitignore                 # Exclusion rules for environments, caches, and logs
├── .github/                   # GitHub templates and community health files
│   ├── ISSUE_TEMPLATE/        # Standardized issue templates (bug, feature, docs)
│   │   ├── bug_report.md
│   │   ├── feature_request.md
│   │   ├── documentation.md
│   │   └── config.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── docs/                      # Central project documentation
│   ├── README.md              # Documentation index
│   ├── architecture/          # System design, data flow, and rule specifications
│   ├── development/           # Setup, virtualenv, and test data generation guide
│   └── operations/            # Prometheus metrics, Grafana dashboards, and alerting
├── data/
│   └── genration_script/      # Deterministic banking test-data generator (stdlib)
│       ├── config.py
│       └── generate_data.py
├── requirements.txt           # Python package dependencies
├── CONTRIBUTING.md            # Contribution guide, branch naming & commit rules
├── CODE_OF_CONDUCT.md         # Team code of conduct and review etiquette
├── SECURITY.md                # Security policy and vulnerability disclosure
├── Data_Quality_Pipeline_Observability_Platform_Documentation.pdf # Full project proposal & plan
└── README.md                  # Project overview (this file)
```

*Note: Component directories (`src/`, `airflow/`, `config/`, `scripts/`, `tests/`) will be populated in accordance with the 6-week project execution timeline.*

---

## Documentation Links

- [Documentation Index](docs/README.md)
- [System Architecture](docs/architecture/README.md)
- [Development Setup Guide](docs/development/README.md)
- [Operations & Telemetry](docs/operations/README.md)
- [Contribution Guidelines](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)
- [Baseline Project Plan & SRS (PDF)](Data_Quality_Pipeline_Observability_Platform_Documentation.pdf)

---

## Team Members & Roles

| Member Name | Role Responsibilities | Abbreviation |
|---|---|---|
| **Mohammad Maher Metwaly** | Data Quality Engineer, DevOps Engineer (Team Leader) | `DQ` – `DO` |
| **Hager Abedlnasser Seddiq** | DevOps Engineer, Data Quality Engineer | `DO` – `DQ` |
| **Elhussieny Sabry Elshahat** | Data Reliability / Test Engineer, Platform / Orchestration Engineer | `QA` – `OR` |
| **Sara Ahmed Abdelhamid** | Platform / Orchestration Engineer, Data Reliability / Test Engineer | `OR` – `QA` |
| **Soltan Akram Soltan** | Data Pipeline Engineer | `DE` |
| **Malak Magdy Mohamed** | Observability Engineer | `OB` |
