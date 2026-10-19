# Data Quality & Pipeline Observability Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%2520Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

</div>

A production-oriented ETL framework that detects data-quality issues early, records failures, exposes operational metrics, and provides monitoring & alerting so problems are caught before they reach business dashboards.

---

## Team

Name | Role/s | Abbreviation/s|
-----|--------|---------------|
Mohammad Maher Metwaly | Data Quality Engineer, DevOps Engineer (Team Leader) | DQ - DO
Hager Abedlnasser Seddiq | DevOps Engineer, Data Quality Engineer | DO - DQ
Elhussieny Sabry Elshahat | Data Reliability /Test Engineer, Data Platform / Orchestration Engineer | QA - OR
Sara Ahmed Abdelhamid | Data Platform / Orchestration Engineer, Data Reliability /Test Engineer | OR - QA
Soltan Akram Soltan | Data Pipeline Engineer | DE
Malak Magdy Mohamed | Observability Engineer | OB

---

## Project Overview

Many companies run automated data pipelines, yet failures and bad data are often discovered only after business users report incorrect dashboards.

This project builds a **representative ETL pipeline** enhanced with:

- **Data-quality checks** (nulls, duplicates, schema/type validation, row counts, referential integrity, range checks)
- **Pipeline reliability** (exception handling, retries, failed-task handling, dead-letter / error records, structured logging)
- **Observability** (pipeline duration, success/failure counts, records processed vs rejected, data-quality failure metrics)
- **Monitoring & Alerting** (Prometheus metrics + Grafana dashboards + alerts for failed or abnormal runs)

The goal is to demonstrate a production-style approach to making data pipelines trustworthy and observable.

---

## Architecture & Data Flow

**High-level flow:**
1. Extract data from source files / database
2. Run comprehensive data-quality validations
3. Route invalid records to an error / dead-letter store
4. Transform clean data and load into the target database
5. Emit metrics (records processed, rejected, duration, DQ failures, pipeline status)
6. Airflow orchestrates the full Extract → Validate → Transform → Load → Quality Check sequence with retries and logging
7. Prometheus scrapes metrics; Grafana visualizes them and triggers alerts on failures or anomalies

---

## Technology Stack

| Layer              | Technology                          |
|--------------------|-------------------------------------|
| Language           | Python 3.10+                        |
| Orchestration      | Apache Airflow                      |
| Data Processing    | Pandas                              |
| Database           | PostgreSQL                          |
| Metrics            | Prometheus                          |
| Visualization      | Grafana                             |
| Containerization   | Docker & Docker Compose             |
| CI/CD              | GitHub Actions                      |
| Logging            | Python `logging` + structured logs  |

---

## Repository Structure

```text
├── airflow/
│   ├── dags/                  # Airflow DAG definitions for pipeline orchestration
│   └── plugins/               # Custom operators and helper hooks
├── config/                    # Database connections, logging, and alert rules
├── data/                      # Sample input data and mock datasets
├── scripts/                   # Setup, migration, and maintenance scripts
├── src/
│   ├── extract/               # Data ingestion modules
│   ├── transform/             # Cleaning, formatting, and business logic
│   ├── validate/              # Data quality rules and schema checking
│   └── load/                  # Database loading and dead-letter routing
├── tests/                     # Unit and integration test suites
├── docker-compose.yml         # Local container orchestration (Airflow, Postgres, Prometheus, Grafana)
├── Dockerfile                 # Custom container build file
├── requirements.txt           # Python dependencies
└── README.md