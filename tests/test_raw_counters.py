from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.validate.raw_counters import CSVProfiler, profile_directories


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


def test_missing_file_raises_exception(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(FileNotFoundError):
        CSVProfiler(missing).analyze()


def test_generation_script_outputs_validation(tmp_path: Path) -> None:
    batch_file = tmp_path / "generated_batch.csv"
    batch_file.write_text(
        "transaction_id,amount,status\n"
        "TX-100,150.50,completed\n"
        "TX-101,200.00,pending\n",
        encoding="utf-8",
    )

    results = profile_directories([str(tmp_path)])

    assert len(results) > 0
    for report in results:
        assert "file_name" in report
        assert "row_count" in report
        assert "duplicated_rows_count" in report
        assert "\\" not in report["file_path"]


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])