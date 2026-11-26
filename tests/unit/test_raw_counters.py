"""Unit tests for the raw counters profiler and CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.validate.raw_counters import CSVProfiler, find_csv_files, main, profile_directories


@pytest.mark.unit
def test_key_dedup_types_dates_and_posix_path(tmp_path: Path) -> None:
    csv_file = tmp_path / "sample_transactions.csv"
    csv_file.write_text(
        "transaction_id,amount,is_flag,tx_date,notes\n"
        "TX-1,100,0,15/01/2020,first\n"
        "TX-1,250,1,01/12/2025,duplicate_key_different_values\n"
        "TX-2,,1,20/06/2023,\n",
        encoding="utf-8",
    )

    report = CSVProfiler(csv_file, key_column="transaction_id").analyze()

    assert report["row_count"] == 3
    assert report["column_count"] == 5
    assert report["duplicated_rows_count"] == 1
    assert report["null_counts_per_column"]["amount"] == 1
    assert report["total_nulls_count"] == 2
    assert report["data_types"]["is_flag"] == "integer"
    assert report["data_types"]["tx_date"] == "datetime"
    assert report["data_ranges"]["tx_date"] == {
        "min": "15/01/2020",
        "max": "01/12/2025",
    }
    assert "\\" not in report["file_path"]


@pytest.mark.unit
def test_blank_keys_do_not_inflate_duplicates(tmp_path: Path) -> None:
    csv_file = tmp_path / "blank_keys.csv"
    csv_file.write_text(
        "transaction_id,amount,notes\n"
        ",100,first_blank_key\n"
        "   ,200,second_blank_key_different_row\n"
        ",100,first_blank_key\n",
        encoding="utf-8",
    )

    report = CSVProfiler(csv_file, key_column="transaction_id").analyze()

    assert report["row_count"] == 3
    assert report["duplicated_rows_count"] == 1


@pytest.mark.unit
def test_missing_file_raises_exception(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(FileNotFoundError):
        CSVProfiler(missing).analyze()


@pytest.mark.unit
def test_empty_csv_and_header_only_csv(tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    report_empty = CSVProfiler(empty).analyze()
    assert report_empty["row_count"] == 0
    assert report_empty["column_count"] == 0

    header_only = tmp_path / "header_only.csv"
    header_only.write_text("col_a,col_b\n", encoding="utf-8")
    report_header = CSVProfiler(header_only).analyze()
    assert report_header["row_count"] == 0
    assert report_header["column_count"] == 2


@pytest.mark.unit
def test_date_formats_and_data_types(tmp_path: Path) -> None:
    csv_file = tmp_path / "all_types.csv"
    csv_file.write_text(
        "id,dt1,dt2,dt3,dt4,is_active,price\n"
        "1,2025-01-01,2025-01-01 10:00:00,12/31/2024,2025-01-01 10:00:00.123456,true,19.99\n"
        "2,2025-01-02,2025-01-02 11:00:00,12/30/2024,2025-01-02 11:00:00.654321,false,29.50\n",
        encoding="utf-8",
    )
    report = CSVProfiler(csv_file, key_column="id").analyze()
    assert report["data_types"]["dt1"] == "datetime"
    assert report["data_types"]["dt2"] == "datetime"
    assert report["data_types"]["dt3"] == "datetime"
    assert report["data_types"]["dt4"] == "datetime"
    assert report["data_types"]["is_active"] == "boolean"
    assert report["data_types"]["price"] == "float"
    assert report["data_ranges"]["price"] == {"min": 19.99, "max": 29.5}


@pytest.mark.unit
def test_find_csv_files_and_profile_directories(tmp_path: Path) -> None:
    sub = tmp_path / "nested"
    sub.mkdir()
    (sub / "a.csv").write_text("x,y\n1,2\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("x,y\n3,4\n", encoding="utf-8")
    (tmp_path / "ignored.txt").write_text("hello", encoding="utf-8")

    files = find_csv_files([str(tmp_path), str(tmp_path / "non_existent")])
    assert len(files) == 2

    results = profile_directories([str(tmp_path)])
    assert len(results) == 2
    for r in results:
        assert "file_name" in r
        assert "row_count" in r
        assert "\\" not in r["file_path"]


@pytest.mark.unit
def test_cli_main_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = tmp_path / "cli_sample.csv"
    csv_file.write_text("transaction_id,amount\nTX-1,50.00\n", encoding="utf-8")
    out_json = tmp_path / "report.json"

    monkeypatch.setattr(
        "sys.argv",
        [
            "raw_counters.py",
            "--dirs",
            str(tmp_path),
            "--key-column",
            "transaction_id",
            "--out",
            str(out_json),
        ],
    )

    main()

    assert out_json.exists()
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["file_name"] == "cli_sample.csv"
    assert data[0]["row_count"] == 1
