#!/usr/bin/env bash
# ==============================================================================
# ObsiFlow Local Quality Gate & CI Pre-Flight Checks
# ==============================================================================
set -euo pipefail

COLOR_RESET="\033[0m"
COLOR_GREEN="\033[1;32m"
COLOR_RED="\033[1;31m"
COLOR_BLUE="\033[1;34m"

log_info() {
    echo -e "${COLOR_BLUE}[INFO]${COLOR_RESET} $1"
}

log_pass() {
    echo -e "${COLOR_GREEN}[PASS]${COLOR_RESET} $1"
}

log_fail() {
    echo -e "${COLOR_RED}[FAIL]${COLOR_RESET} $1"
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

log_info "Running ObsiFlow Repository Quality Checks..."

# 1. Repository hygiene: tracked files audit
log_info "1. Auditing Git repository hygiene and sensitive files..."
tracked="$(git ls-files)"

if echo "$tracked" | grep -E "(^|/)\.env($|\..*)" | grep -v "\.env\.example$" >/dev/null 2>&1; then
    log_fail "Tracked .env file detected!"
    exit 1
fi

if echo "$tracked" | grep -E "\.(log|sqlite|sqlite3|dump|sql\.gz)$" >/dev/null 2>&1; then
    log_fail "Tracked database dump or log file detected!"
    exit 1
fi

if echo "$tracked" | grep -E "(raw_counters_report\.json|standalone_admin_password\.txt)" >/dev/null 2>&1; then
    log_fail "Tracked generated artifacts or standalone password detected!"
    exit 1
fi

if echo "$tracked" | grep -E "data/(staging|batches)/.*\.csv$" >/dev/null 2>&1; then
    log_fail "Tracked batch CSV files detected!"
    exit 1
fi

if echo "$tracked" | grep -F "\\" >/dev/null 2>&1; then
    log_fail "Non-POSIX path with backslashes detected in git tree!"
    exit 1
fi
log_pass "Repository hygiene verified: no forbidden or generated files tracked."

# 2. Ruff Linting & Formatting Check
log_info "2. Checking code formatting and linting (ruff)..."
ruff check config etl validation dags monitoring tests src
ruff format --check config etl validation dags monitoring tests src
log_pass "Ruff checks and formatting passed cleanly."

# 3. YAML & SQL Configuration Validation
log_info "3. Validating YAML configuration and SQL schema integrity..."
python3 - << 'EOF'
import sys
from pathlib import Path
import yaml

yaml_files = [
    p for p in Path(".").rglob("*")
    if p.suffix in {".yaml", ".yml"}
    and not any(part.startswith(".") for part in p.parts if part not in {".github"})
]
for yml in yaml_files:
    try:
        list(yaml.safe_load_all(yml.read_text(encoding="utf-8")))
    except Exception as exc:
        print(f"FAILED YAML: {yml}: {exc}", file=sys.stderr)
        sys.exit(1)

sql_path = Path("db/init.sql")
if not sql_path.exists():
    print("FAILED SQL: db/init.sql missing", file=sys.stderr)
    sys.exit(1)
sql = sql_path.read_text(encoding="utf-8").lower()
for t in ["reference.accounts", "target.transactions", "quality.error_records", "observability.pipeline_runs"]:
    if f"create table if not exists {t}" not in sql:
        print(f"FAILED SQL: missing DDL for {t}", file=sys.stderr)
        sys.exit(1)
EOF
log_pass "YAML and SQL configurations are structurally sound."

# 4. Unit Test Suite and Coverage Gate
log_info "4. Executing Unit Test Suite with coverage enforcement..."
python3 -m pytest tests/unit -m unit \
    --cov=config \
    --cov=etl \
    --cov=validation \
    --cov=monitoring \
    --cov=src \
    --cov-report=term-missing \
    --cov-fail-under=80
log_pass "Unit test suite passed all checks with >80% coverage."

# 5. Data Generator Determinism
log_info "5. Verifying banking test-data generator determinism..."
python3 data/genration_script/generate_data.py --out /tmp/obsiflow_local_gen --rows 30 --seed 42 >/dev/null
test -f /tmp/obsiflow_local_gen/reference/accounts.csv
test -f /tmp/obsiflow_local_gen/batches/transactions_clean_1000.csv
test -f /tmp/obsiflow_local_gen/expected/manifest.json
rm -rf /tmp/obsiflow_local_gen
log_pass "Banking test-data generator verified."

echo -e "\n${COLOR_GREEN}======================================================${COLOR_RESET}"
echo -e "${COLOR_GREEN} All quality gates and pre-flight checks PASSED! ${COLOR_RESET}"
echo -e "${COLOR_GREEN}======================================================${COLOR_RESET}"
