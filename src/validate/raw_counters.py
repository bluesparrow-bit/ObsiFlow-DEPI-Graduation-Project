"""
Task: OFP-3 - Raw Counters
Description:
    Count Rows, Columns, Nulls, Duplication, 
    Data Ranges, Data Types, and Raw Columns Order.
"""
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S.%f",
)


def _is_int(val: str) -> bool:
    try:
        int(val)
        return True
    except ValueError:
        return False


def _is_float(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


def _is_bool(val: str) -> bool:
    return val.lower() in {"true", "false", "1", "0", "yes", "no"}


def _is_date(val: str) -> bool:
    for fmt in DATE_FORMATS:
        try:
            datetime.strptime(val, fmt)
            return True
        except ValueError:
            pass
    return False


def infer_column_type(values: List[str]) -> str:
    cleaned = [v.strip() for v in values if v and v.strip()]
    if not cleaned:
        return "empty"

    total = len(cleaned)
    counts = {"int": 0, "float": 0, "bool": 0, "date": 0}

    for val in cleaned:
        if _is_int(val):
            counts["int"] += 1
        elif _is_float(val):
            counts["float"] += 1
        elif _is_bool(val):
            counts["bool"] += 1

        if _is_date(val):
            counts["date"] += 1

    if counts["int"] == total:
        return "integer"
    if counts["int"] + counts["float"] == total:
        return "float"
    if counts["bool"] == total:
        return "boolean"
    if counts["date"] == total:
        return "datetime"
    return "string"


def get_column_range(values: List[str], col_type: str) -> Dict[str, Optional[Any]]:
    cleaned = [v.strip() for v in values if v and v.strip()]
    if not cleaned:
        return {"min": None, "max": None}

    if col_type in ("integer", "float"):
        cast_fn = int if col_type == "integer" else float
        nums = [cast_fn(v) for v in cleaned]
        return {"min": min(nums), "max": max(nums)}

    sorted_vals = sorted(cleaned)
    return {"min": sorted_vals[0], "max": sorted_vals[-1]}


class CSVProfiler:
    """Analyzes structure, data types, null rates, and metrics of a CSV file."""

    def __init__(self, file_path: Path):
        self.path = Path(file_path)

    def _hash_row(self, row: List[str]) -> str:
        raw = "||".join(f.strip() for f in row)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def analyze(self) -> Optional[Dict[str, Any]]:
        if not self.path.is_file():
            return None

        try:
            with self.path.open(mode="r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    return self._empty_report()

                columns = [h.strip() for h in headers]
                col_data: Dict[str, List[str]] = {col: [] for col in columns}
                seen_hashes = set()
                dupes = 0
                row_count = 0

                for row in reader:
                    if not row or not any(field.strip() for field in row):
                        continue

                    row_count += 1
                    row_hash = self._hash_row(row)
                    if row_hash in seen_hashes:
                        dupes += 1
                    else:
                        seen_hashes.add(row_hash)

                    for idx, col in enumerate(columns):
                        val = row[idx] if idx < len(row) else ""
                        col_data[col].append(val)

                null_counts = {}
                data_types = {}
                data_ranges = {}
                total_nulls = 0

                for col, vals in col_data.items():
                    null_cnt = sum(1 for v in vals if not v or not v.strip())
                    null_counts[col] = null_cnt
                    total_nulls += null_cnt

                    col_type = infer_column_type(vals)
                    data_types[col] = col_type
                    data_ranges[col] = get_column_range(vals, col_type)

                return {
                    "file_name": self.path.name,
                    "file_path": str(self.path),
                    "row_count": row_count,
                    "column_count": len(columns),
                    "raw_columns_order": columns,
                    "data_types": data_types,
                    "null_counts_per_column": null_counts,
                    "total_nulls_count": total_nulls,
                    "duplicated_rows_count": dupes,
                    "data_ranges": data_ranges,
                }

        except Exception:
            return None

    def _empty_report(self) -> Dict[str, Any]:
        return {
            "file_name": self.path.name,
            "file_path": str(self.path),
            "row_count": 0,
            "column_count": 0,
            "raw_columns_order": [],
            "data_types": {},
            "null_counts_per_column": {},
            "total_nulls_count": 0,
            "duplicated_rows_count": 0,
            "data_ranges": {},
        }


def find_csv_files(directories: List[str]) -> List[Path]:
    files = []
    for dir_path in directories:
        target = Path(dir_path)
        if target.exists():
            files.extend(target.rglob("*.csv"))
    return files


def print_summary_table(results: List[Dict[str, Any]]) -> None:
    headers = ["File Name", "Rows", "Cols", "Total Nulls", "Duplicates", "Columns Order"]
    rows = []
    for r in results:
        cols_str = ", ".join(r["raw_columns_order"])
        if len(cols_str) > 30:
            cols_str = cols_str[:27] + "..."
        rows.append([
            r["file_name"],
            str(r["row_count"]),
            str(r["column_count"]),
            str(r["total_nulls_count"]),
            str(r["duplicated_rows_count"]),
            cols_str,
        ])

    col_widths = [max(len(h), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]
    row_format = " | ".join(f"{{:<{w}}}" for w in col_widths)

    print(row_format.format(*headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in rows:
        print(row_format.format(*row))


def run_pipeline(target_directories: List[str], json_output_path: str = "raw_counters_report.json") -> List[Dict[str, Any]]:
    csv_files = find_csv_files(target_directories)
    results = []

    for file_path in csv_files:
        report = CSVProfiler(file_path).analyze()
        if report:
            results.append(report)

    print_summary_table(results)

    with open(json_output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    return results


if __name__ == "__main__":
    target_dirs = ["data/batches", "data/reference"]
    run_pipeline(target_dirs)