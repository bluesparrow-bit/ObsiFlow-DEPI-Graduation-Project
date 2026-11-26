#!/usr/bin/env bash
# ==============================================================================
# ObsiFlow Local Quality Gate & CI Pre-Flight Checks
# ==============================================================================
set -euo pipefail

COLOR_RESET="\033[0m"
COLOR_GREEN="\033[1;32m"
COLOR_RED="\033[1;31m"
COLOR_YELLOW="\033[1;33m"
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

# 1. Repository hygiene: single source of truth via check_hygiene.py
log_info "1. Auditing Git repository hygiene and sensitive files..."
python3 scripts/check_hygiene.py
log_pass "Repository hygiene verified: no forbidden or generated files tracked."

# 2. Ruff Linting & Formatting Check
log_info "2. Checking code formatting and linting (ruff)..."
RUFF_VER=$(ruff --version 2>/dev/null | awk '{print $2}' || echo "unknown")
if [[ "${RUFF_VER}" != "0.7.4" ]]; then
    echo -e "${COLOR_YELLOW}[WARN]${COLOR_RESET} Local ruff version (${RUFF_VER}) differs from CI (0.7.4). Recommend: pip install 'ruff==0.7.4'"
fi
ruff check config etl validation dags monitoring tests src scripts
ruff format --check config etl validation dags monitoring tests src scripts
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
rm -rf /tmp/obsiflow_gen_a /tmp/obsiflow_gen_b /tmp/obsiflow_gen_c
python3 data/genration_script/generate_data.py --out /tmp/obsiflow_gen_a --rows 1000 --seed 42 >/dev/null
python3 data/genration_script/generate_data.py --out /tmp/obsiflow_gen_b --rows 1000 --seed 42 >/dev/null
diff -r /tmp/obsiflow_gen_a /tmp/obsiflow_gen_b >/dev/null

python3 data/genration_script/generate_data.py --out /tmp/obsiflow_gen_c --rows 1000 --seed 99 >/dev/null
if cmp -s /tmp/obsiflow_gen_a/batches/transactions_clean_1000.csv /tmp/obsiflow_gen_c/batches/transactions_clean_1000.csv; then
    log_fail "Generator produced identical output with different seeds!"
    exit 1
fi
rm -rf /tmp/obsiflow_gen_a /tmp/obsiflow_gen_b /tmp/obsiflow_gen_c
log_pass "Banking test-data generator determinism verified."

echo -e "\n${COLOR_GREEN}======================================================${COLOR_RESET}"
echo -e "${COLOR_GREEN} All quality gates and pre-flight checks PASSED! ${COLOR_RESET}"
echo -e "${COLOR_GREEN}======================================================${COLOR_RESET}"
