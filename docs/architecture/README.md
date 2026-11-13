# System Architecture

## High-Level Data Flow

ObsiFlow is designed as an observable, fault-tolerant ETL framework. The core processing pipeline proceeds through the following stages:

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

## Data Quality Rule Families

The validation engine evaluates incoming transactions across six distinct rule families before loading:

| # | Rule Family | Scope & Validation Objective |
|---|---|---|
| 1 | **Null-Value Validation** | Detects missing values in mandatory fields (`account_id`, `txn_type`, `amount`, `txn_date`). |
| 2 | **Duplicate Detection** | Flags repeated records by primary key (`transaction_id`) or business key combination. |
| 3 | **Schema & Type Validation** | Verifies column completeness, data types, and formatting conventions. |
| 4 | **Row-Count Validation** | Ensures batches fall within acceptable row-count limits (e.g., non-empty, expected batch size). |
| 5 | **Referential Integrity** | Confirms foreign key references exist in reference data (`account_id` in `accounts.csv`). |
| 6 | **Range Validation** | Asserts numerical amounts and transaction dates fall within acceptable operational bounds (`AMOUNT_MIN` to `AMOUNT_MAX`, valid date boundaries). |

---

## Quarantine & Dead-Letter Store

- Records failing any validation rule are routed to an isolated quarantine store (file or database table).
- Quarantined records preserve original row contents, failure timestamps, batch identifiers, and the specific rule family failure reason.
- Clean records continue downstream without pipeline interruption unless the overall batch failure rate exceeds the warning threshold (10%).

---

## Database Architecture & Team Decision Note

> [!NOTE]
> **Audit Finding — Database Technology Synchronization**:
> - The initial Project Proposal PDF contains a discrepancy: Section 5 lists `MS SQL Server / T-SQL`, whereas Section 3.1, the Project Plan, KPI specifications, Functional Requirements, and the repository `README.md` specify `PostgreSQL`.
> - The engineering team should formally record this architectural decision. PostgreSQL is the baseline assumed throughout the pipeline logging, Airflow metadata, and Docker Compose designs.
