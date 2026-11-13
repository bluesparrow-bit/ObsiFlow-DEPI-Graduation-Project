# ObsiFlow Documentation Index

Welcome to the documentation for **ObsiFlow (Data Quality & Pipeline Observability Platform)**. This directory organizes technical documentation, architectural designs, development workflows, and operational procedures for the project team.

---

## Directory Organization

```text
docs/
├── architecture/         # System architecture, data flow, rule families, and design decisions
│   └── README.md
├── development/          # Local setup, environment configuration, and coding standards
│   └── README.md
├── operations/           # Telemetry specifications, Prometheus metrics, and Grafana alerting
│   └── README.md
└── README.md             # Documentation index (this file)
```

---

## Key Documentation Links

- **Architecture & System Design**: [architecture/README.md](architecture/README.md)
  - Data ingestion and validation pipeline flow
  - Six data quality rule families
  - Rejected-record quarantine model
  - Database technology architecture notes

- **Development Guide**: [development/README.md](development/README.md)
  - Prerequisites (Python 3.10+)
  - Virtual environment and dependency management
  - Deterministic banking test-data generation

- **Operations & Observability**: [operations/README.md](operations/README.md)
  - Operational metrics and Prometheus export format
  - Planned alert scenarios (stale, failed, never-reported, high-rejection)
  - Pipeline execution logging schema

- **Repository Governance**:
  - [Contribution Guidelines](../CONTRIBUTING.md)
  - [Code of Conduct](../CODE_OF_CONDUCT.md)
  - [Security Policy](../SECURITY.md)

---

## Baseline Specifications

The authoritative project proposal, 6-week execution plan, task matrix (DE, DQ, OR, OB, QA, DO), and KPI measurement criteria are detailed in the repository documentation PDF:
- [Data_Quality_Pipeline_Observability_Platform_Documentation.pdf](../Data_Quality_Pipeline_Observability_Platform_Documentation.pdf)
