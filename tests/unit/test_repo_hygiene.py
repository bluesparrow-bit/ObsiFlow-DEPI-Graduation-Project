"""Automated tests enforcing repository hygiene, configuration integrity, and standards."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from scripts.check_hygiene import (
    check_posix_paths,
    find_conflict_markers,
    find_forbidden_files,
    get_tracked_files,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_no_sensitive_or_generated_files_tracked() -> None:
    tracked_files = get_tracked_files(PROJECT_ROOT)
    violations = find_forbidden_files(tracked_files)
    assert not violations, f"Forbidden or sensitive files tracked in git: {violations}"

    non_posix = check_posix_paths(tracked_files)
    assert not non_posix, f"Non-POSIX paths found: {non_posix}"


@pytest.mark.unit
def test_no_unresolved_merge_conflicts() -> None:
    conflicts = find_conflict_markers(PROJECT_ROOT)
    assert not conflicts, f"Unresolved merge conflict markers detected in: {conflicts}"


@pytest.mark.unit
def test_hygiene_detection_rules_synthetic() -> None:
    synthetic = [
        ".env",
        ".env.local",
        ".env.example",
        "data/sample_valid.csv",
        "data/sample_corrupted.csv",
        "data/staging/batch_01.csv",
        "data/batches/test.csv",
        "data/genration_script/output/dump.csv",
        "dump.sqlite3",
        "debug.log",
        "raw_counters_report.json",
        "standalone_admin_password.txt",
    ]
    forbidden = find_forbidden_files(synthetic)
    assert ".env" in forbidden
    assert ".env.local" in forbidden
    assert ".env.example" not in forbidden
    assert "data/sample_valid.csv" not in forbidden
    assert "data/sample_corrupted.csv" not in forbidden
    assert "data/staging/batch_01.csv" in forbidden
    assert "data/batches/test.csv" in forbidden
    assert "data/genration_script/output/dump.csv" in forbidden
    assert "dump.sqlite3" in forbidden
    assert "debug.log" in forbidden
    assert "raw_counters_report.json" in forbidden
    assert "standalone_admin_password.txt" in forbidden

    assert check_posix_paths(["a/b/c.py", "a\\b\\c.py"]) == ["a\\b\\c.py"]


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
