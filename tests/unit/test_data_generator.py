"""Unit tests for the banking data generator."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.unit
def test_generator_deterministic_output(tmp_path: Path) -> None:
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"

    cmd1 = [
        sys.executable,
        "data/genration_script/generate_data.py",
        "--out",
        str(out1),
        "--rows",
        "30",
        "--accounts",
        "10",
        "--seed",
        "123",
    ]
    cmd2 = [
        sys.executable,
        "data/genration_script/generate_data.py",
        "--out",
        str(out2),
        "--rows",
        "30",
        "--accounts",
        "10",
        "--seed",
        "123",
    ]

    res1 = subprocess.run(cmd1, capture_output=True, text=True, check=True)
    res2 = subprocess.run(cmd2, capture_output=True, text=True, check=True)

    assert res1.returncode == 0
    assert res2.returncode == 0

    clean1 = out1 / "batches" / "transactions_clean_1000.csv"
    clean2 = out2 / "batches" / "transactions_clean_1000.csv"
    assert clean1.exists()
    assert clean2.exists()
    assert _hash_file(clean1) == _hash_file(clean2)

    manifest1 = json.loads((out1 / "expected" / "manifest.json").read_text(encoding="utf-8"))
    manifest2 = json.loads((out2 / "expected" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest1 == manifest2
    assert "batches" in manifest1
    assert "clean" in manifest1["batches"]
    assert manifest1["batches"]["clean"]["total_rows"] == 30


@pytest.mark.unit
def test_generator_creates_all_expected_fixtures(tmp_path: Path) -> None:
    out = tmp_path / "fixtures"
    cmd = [
        sys.executable,
        "data/genration_script/generate_data.py",
        "--out",
        str(out),
        "--rows",
        "20",
        "--seed",
        "42",
    ]
    subprocess.run(cmd, check=True)

    assert (out / "reference" / "accounts.csv").exists()
    assert (out / "batches" / "transactions_clean_1000.csv").exists()
    assert (out / "batches" / "transactions_high_rejection.csv").exists()
    assert (out / "batches" / "transactions_schema_corrupt.csv").exists()
    assert (out / "batches" / "transactions_schema_corrupt_fixed.csv").exists()
    assert (out / "batches" / "transactions_empty.csv").exists()
    assert (out / "batches" / "transactions_unreadable.csv").exists()
    assert (out / "expected" / "manifest.json").exists()

    corrupt_file = out / "batches" / "transactions_schema_corrupt.csv"
    corrupt_lines = corrupt_file.read_text(encoding="utf-8").splitlines()
    assert "amount" not in corrupt_lines[0]
    assert "acct_no" in corrupt_lines[0]
