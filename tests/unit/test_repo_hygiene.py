"""Automated tests enforcing repository hygiene, configuration integrity, and standards."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_no_sensitive_or_generated_files_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked_files = result.stdout.splitlines()

    forbidden_exact = {
        ".env",
        "raw_counters_report.json",
        "standalone_admin_password.txt",
    }
    forbidden_suffixes = (
        ".log",
        ".sqlite",
        ".sqlite3",
        ".dump",
        ".sql.gz",
    )

    def _is_forbidden(file_path: str) -> bool:
        name = Path(file_path).name
        if name in forbidden_exact or name.endswith(forbidden_suffixes):
            return True
        if name.startswith(".env.") and name != ".env.example":
            return True
        if "data/staging" in file_path and file_path.endswith(".csv"):
            return True
        return "data/genration_script/output" in file_path

    violations = [f for f in tracked_files if _is_forbidden(f)]

    assert not violations, f"Forbidden or sensitive files tracked in git: {violations}"


@pytest.mark.unit
def test_all_yaml_files_are_valid() -> None:
    yaml_files = [
        p
        for p in PROJECT_ROOT.rglob("*")
        if p.suffix in {".yaml", ".yml"}
        and not any(part.startswith(".") for part in p.parts if part not in {".github"})
    ]

    assert len(yaml_files) > 0

    for yml in yaml_files:
        content = yml.read_text(encoding="utf-8")
        try:
            # Multi-document files like workflow or grafana provision
            list(yaml.safe_load_all(content))
        except Exception as exc:
            pytest.fail(f"YAML parsing failed for {yml}: {exc}")


@pytest.mark.unit
def test_sql_schema_defines_all_required_objects() -> None:
    sql_path = PROJECT_ROOT / "db" / "init.sql"
    assert sql_path.exists(), "db/init.sql is missing"
    sql = sql_path.read_text(encoding="utf-8").lower()

    required_schemas = ["quality", "reference", "observability", "target"]
    for s in required_schemas:
        assert f"create schema if not exists {s}" in sql

    required_tables = [
        "reference.accounts",
        "target.transactions",
        "quality.error_records",
        "observability.pipeline_runs",
    ]
    for t in required_tables:
        assert f"create table if not exists {t}" in sql


@pytest.mark.unit
def test_all_unit_tests_have_marker() -> None:
    unit_dir = PROJECT_ROOT / "tests" / "unit"
    test_files = list(unit_dir.glob("test_*.py"))

    missing_marker: list[str] = []
    for f in test_files:
        tree = ast.parse(f.read_text(encoding="utf-8"), filename=str(f))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                has_unit_mark = any(
                    (isinstance(dec, ast.Attribute) and dec.attr == "unit")
                    or (isinstance(dec, ast.Call) and getattr(dec.func, "attr", None) == "unit")
                    for dec in node.decorator_list
                )
                if not has_unit_mark:
                    missing_marker.append(f"{f.name}::{node.name}")

    assert (
        not missing_marker
    ), f"The following unit tests are missing the @pytest.mark.unit decorator: {missing_marker}"
