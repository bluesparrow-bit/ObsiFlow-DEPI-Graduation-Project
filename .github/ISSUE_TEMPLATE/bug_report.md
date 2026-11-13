---
name: Bug Report
about: Report an unexpected error, pipeline malfunction, or failure
title: '[BUG]: '
labels: 'bug'
assignees: ''
---

### Bug Description

A clear and concise description of the bug encountered.

### Affected Component

- [ ] Data Ingestion / Extraction (`src/extract/`)
- [ ] Data Quality Validation (`src/validate/`)
- [ ] Transformation Logic (`src/transform/`)
- [ ] Data Loading & Quarantine (`src/load/`)
- [ ] Airflow Orchestration / DAGs (`airflow/`)
- [ ] Metrics & Telemetry (Prometheus / Grafana)
- [ ] Synthetic Data Generator (`data/genration_script/`)
- [ ] Project Configuration / Environment

### Steps to Reproduce

1. Go to '...'
2. Run command '...'
3. See error '...'

### Expected Behavior

A clear and concise description of what was expected to happen.

### Actual Behavior / Error Logs

Include the terminal output, Airflow task log, or traceback:
```text
# Paste error message or logs here
```

### Environment Details

- OS: [e.g., Ubuntu 22.04, macOS, Windows/WSL2]
- Python Version: [e.g., 3.10.12]
- Related Task ID: [e.g., DE-02, DQ-04]

### Additional Context

Add any other context, sample batch details, or screenshots about the problem here.
