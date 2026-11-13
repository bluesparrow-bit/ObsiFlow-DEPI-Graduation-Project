# Development Guide

This guide describes how to set up your local development environment and run development utilities for the ObsiFlow project.

---

## Prerequisites

- **Python**: Version 3.10 or higher
- **Git**: For version control and collaborative branching
- **Make / Bash**: Standard Unix shell environment or WSL2 on Windows

---

## Local Setup

### 1. Clone Repository & Setup Virtual Environment

```bash
# Clone the repository
git clone https://github.com/bluesparrow-bit/ObsiFlow-DEPI-Graduation-Project.git
cd ObsiFlow-DEPI-Graduation-Project

# Create and activate a Python virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2. Dependencies

The project uses `requirements.txt` for pinning project dependencies. As components are integrated, install requirements via:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Test Data Generator

The repository includes a deterministic synthetic data generator in `data/genration_script/generate_data.py` that utilizes only the Python standard library. It generates sample reference accounts and transaction batches with deterministic defect mixes across all six rule families.

### Generating Test Batches

```bash
# Generate 1,000 rows with default seed 42 into an output directory
python data/genration_script/generate_data.py --out ./data_output --rows 1000 --seed 42
```

Outputs include:
- `reference/accounts.csv`: 500 reference banking accounts.
- `batches/transactions_clean_1000.csv`: Batch with standard baseline defects (5% rejection rate).
- `batches/transactions_high_rejection.csv`: Batch triggering alert thresholds (15% rejection rate).
- `batches/transactions_schema_corrupt.csv`: Schema corruption scenario.
- `batches/transactions_empty.csv`: Zero-row batch scenario.
- `expected/manifest.json`: Exact expected record counts and defect distributions.

---

## Coding Standards & Governance

- Follow `.editorconfig` rules for indentation (4 spaces for Python, 2 spaces for YAML/JSON/Markdown) and LF line endings.
- Consult [CONTRIBUTING.md](file:///run/media/elhussieny/New%20Volume/Data-Engineer/DEPI/FinalProject/ObsiFlow/CONTRIBUTING.md) for branch naming, Conventional Commits, pull request workflows, and review guidelines.
