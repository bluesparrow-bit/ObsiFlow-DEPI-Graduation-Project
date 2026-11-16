"""
Task: OFP-3 - Raw Counters
Description:
    Count Rows, Columns, Nulls, Duplication, 
    Data Ranges, Data Types, and Raw Columns Order.
"""
import argparse
import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

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
    return val.lower() in {"true", "false", "yes", "no"}


def _parse_date(val: str) -> Optional[datetime]:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


@dataclass
class _ColumnStreamStats:
    """Tracks column metrics incrementally in O(1) memory per column."""

    non_null_count: int = 0
    null_count: int = 0
    int_count: int = 0
    float_count: int = 0
    bool_count: int = 0
    date_count: int = 0

    num_min: Optional[float] = None
    num_max: Optional[float] = None
    date_min: Optional[Tuple[datetime, str]] = None
    date_max: Optional[Tuple[datetime, str]] = None
    str_min: Optional[str] = None
    str_max: Optional[str] = None

    def update(self, raw_val: str) -> None:
        val = raw_val.strip() if raw_val else ""
        if not val:
            self.null_count += 1
            return

        self.non_null_count += 1

        if self.str_min is None or val < self.str_min:
            self.str_min = val
        if self.str_max is None or val > self.str_max:
            self.str_max = val

        if _is_int(val):
            self.int_count += 1
            self._update_num_bounds(int(val))
        elif _is_float(val):
            self.float_count += 1
            self._update_num_bounds(float(val))
        elif _is_bool(val):
            self.bool_count += 1

        parsed_dt = _parse_date(val)
        if parsed_dt is not None:
            self.date_count += 1
            if self.date_min is None or parsed_dt < self.date_min[0]:
                self.date_min = (parsed_dt, val)
            if self.date_max is None or parsed_dt > self.date_max[0]:
                self.date_max = (parsed_dt, val)

    def _update_num_bounds(self, num: Union[int, float]) -> None:
        if self.num_min is None or num < self.num_min:
            self.num_min = num
        if self.num_max is None or num > self.num_max:
            self.num_max = num

    def infer_type(self) -> str:
        total = self.non_null_count
        if total == 0:
            return "empty"
        if self.int_count == total:
            return "integer"
        if self.int_count + self.float_count == total:
            return "float"
        if self.bool_count == total:
            return "boolean"
        if self.date_count == total:
            return "datetime"
        return "string"

    def get_range(self, col_type: str) -> Dict[str, Optional[Any]]:
        if self.non_null_count == 0:
            return {"min": None, "max": None}

        if col_type == "integer":
            return {
                "min": int(self.num_min) if self.num_min is not None else None,
                "max": int(self.num_max) if self.num_max is not None else None,
            }
        if col_type == "float":
            return {
                "min": float(self.num_min) if self.num_min is not None else None,
                "max": float(self.num_max) if self.num_max is not None else None,
            }
        if col_type == "datetime":
            return {
                "min": self.date_min[1] if self.date_min else None,
                "max": self.date_max[1] if self.date_max else None,
            }
        return {"min": self.str_min, "max": self.str_max}


class CSVProfiler:
    """Streaming CSV profiler designed for Airflow XCom and PostgreSQL audit logging."""

    def __init__(
        self,
        file_path: Union[str, Path],
        key_column: Optional[str] = "transaction_id",
    ):
        self.path = Path(file_path)
        self.key_column = key_column

    def _hash_row(self, row: List[str]) -> str:
        raw = "||".join(f.strip() for f in row)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def analyze(self) -> Dict[str, Any]:
        if not self.path.is_file():
            raise FileNotFoundError(f"CSV file not found: {self.path.as_posix()}")

        try:
            with self.path.open(mode="r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    return self._empty_report()

                columns = [h.strip() for h in headers]
                col_stats: Dict[str, _ColumnStreamStats] = {
                    col: _ColumnStreamStats() for col in columns
                }

                use_key_dedup = bool(self.key_column and self.key_column in columns)
                key_idx = columns.index(self.key_column) if use_key_dedup else -1

                seen_keys: set[str] = set()
                dupes = 0
                row_count = 0

                for row in reader:
                    if not row or not any(field.strip() for field in row):
                        continue

                    row_count += 1

                    if use_key_dedup:
                        dedup_token = row[key_idx].strip() if key_idx < len(row) else ""
                    else:
                        dedup_token = self._hash_row(row)

                    if dedup_token:
                        if dedup_token in seen_keys:
                            dupes += 1
                        else:
                            seen_keys.add(dedup_token)
                    else:
                        row_hash = self._hash_row(row)
                        if row_hash in seen_keys:
                            dupes += 1
                        else:
                            seen_keys.add(row_hash)

                    for idx, col in enumerate(columns):
                        val = row[idx] if idx < len(row) else ""
                        col_stats[col].update(val)

                null_counts: Dict[str, int] = {}
                data_types: Dict[str, str] = {}
                data_ranges: Dict[str, Dict[str, Optional[Any]]] = {}
                total_nulls = 0

                for col in columns:
                    stats = col_stats[col]
                    null_counts[col] = stats.null_count
                    total_nulls += stats.null_count

                    inferred_type = stats.infer_type()
                    data_types[col] = inferred_type
                    data_ranges[col] = stats.get_range(inferred_type)

                return {
                    "file_name": self.path.name,
                    "file_path": self.path.as_posix(),
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
            logger.exception("Failed to profile CSV file: %s", self.path.as_posix())
            raise

    def _empty_report(self) -> Dict[str, Any]:
        return {
            "file_name": self.path.name,
            "file_path": self.path.as_posix(),
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
    files: List[Path] = []
    for dir_path in directories:
        target = Path(dir_path)
        if target.exists():
            files.extend(sorted(target.rglob("*.csv")))
    return files


def profile_directories(
    target_directories: List[str],
    key_column: Optional[str] = "transaction_id",
) -> List[Dict[str, Any]]:
    """Pure library entrypoint for Airflow tasks (returns XCom-serializable dicts)."""
    csv_files = find_csv_files(target_directories)
    return [
        CSVProfiler(file_path, key_column=key_column).analyze()
        for file_path in csv_files
    ]


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

    col_widths = [
        max(len(h), max((len(r[i]) for r in rows), default=0))
        for i, h in enumerate(headers)
    ]
    row_format = " | ".join(f"{{:<{w}}}" for w in col_widths)

    print(row_format.format(*headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in rows:
        print(row_format.format(*row))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Profile raw CSV files for ingestion counters and schema metrics."
    )
    parser.add_argument(
        "--dirs",
        nargs="+",
        default=["data/batches", "data/reference"],
        help="Target directories to scan recursively for CSV files.",
    )
    parser.add_argument(
        "--key-column",
        default="transaction_id",
        help="Column name used for key-level duplicate detection (falls back to full-row hash if absent).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output path to write the JSON profiling report.",
    )
    args = parser.parse_args()

    results = profile_directories(args.dirs, key_column=args.key_column)
    print_summary_table(results)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        logger.info("Profiling report written to %s", args.out.as_posix())


if __name__ == "__main__":
    main()