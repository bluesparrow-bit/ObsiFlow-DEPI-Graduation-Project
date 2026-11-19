"""End-to-end ETL flow against real PostgreSQL: valid, mixed, corrupt, empty."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest
from psycopg2.extras import RealDictCursor

from config.settings import Settings
from etl.contracts import PermanentPipelineError, RunStatus
from etl.pipeline import finalize_run_stage, run_pipeline

pytestmark = pytest.mark.integration


def _query_one(connection: Any, query: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, parameters)
        row = cursor.fetchone()
    connection.commit()
    return dict(row) if row else {}


def _query_all(
    connection: Any, query: str, parameters: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, parameters)
        rows = [dict(row) for row in cursor.fetchall()]
    connection.commit()
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    columns = [
        "transaction_id",
        "account_id",
        "txn_type",
        "amount",
        "currency",
        "txn_date",
        "channel",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _row(index: int, **overrides: str) -> dict[str, str]:
    row = {
        "transaction_id": f"TXN7{index:07d}",
        "account_id": f"ACC{(index - 1) % 10 + 1:06d}",
        "txn_type": "DEPOSIT",
        "amount": "100.00",
        "currency": "USD",
        "txn_date": "2025-01-15",
        "channel": "ONLINE",
        "status": "POSTED",
    }
    row.update(overrides)
    return row


def test_valid_batch_loads_and_is_idempotent(
    clean_db: Any,
    settings: Settings,
    project_root: Path,
) -> None:
    source = project_root / "data" / "sample_valid.csv"

    first = run_pipeline(source, run_id="it-valid-1", settings=settings, emit_metrics=False)
    second = run_pipeline(source, run_id="it-valid-2", settings=settings, emit_metrics=False)

    assert first.status is RunStatus.SUCCESS
    assert second.status is RunStatus.SUCCESS
    assert first.records_extracted == 20
    assert second.records_loaded == 20

    target = _query_one(clean_db, "SELECT COUNT(*) AS total FROM target.transactions")
    distinct = _query_one(
        clean_db,
        "SELECT COUNT(DISTINCT transaction_id) AS total FROM target.transactions",
    )
    assert target["total"] == 20
    assert distinct["total"] == 20

    runs = _query_all(
        clean_db,
        "SELECT status, records_loaded FROM observability.pipeline_runs "
        "WHERE run_id IN (%s, %s) ORDER BY run_id",
        ("it-valid-1", "it-valid-2"),
    )
    assert [run["status"] for run in runs] == ["SUCCESS", "SUCCESS"]
    assert [run["records_loaded"] for run in runs] == [20, 20]


def test_reusing_run_id_resets_stale_audit_counters(
    clean_db: Any,
    settings: Settings,
    project_root: Path,
) -> None:
    source = project_root / "data" / "sample_valid.csv"
    with clean_db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO observability.pipeline_runs "
            "(run_id, dag_id, source_file, status, records_extracted, records_valid, "
            " records_loaded, records_rejected, rejection_rate, failures_by_rule, error_summary) "
            "VALUES (%s, %s, %s, 'FAILED', 999, 0, 0, 999, 1.0, %s, 'stale failure')",
            ("it-restart-1", settings.pipeline_dag_id, "stale.csv", '{"null": 999}'),
        )
    clean_db.commit()

    run = run_pipeline(source, run_id="it-restart-1", settings=settings, emit_metrics=False)

    assert run.status is RunStatus.SUCCESS
    row = _query_one(
        clean_db,
        "SELECT * FROM observability.pipeline_runs WHERE run_id = %s",
        ("it-restart-1",),
    )
    assert row["status"] == "SUCCESS"
    assert row["records_extracted"] == 20
    assert row["records_rejected"] == 0
    assert row["records_loaded"] == 20
    assert float(row["rejection_rate"]) == 0.0
    assert row["error_summary"] is None
    assert all(int(count) == 0 for count in row["failures_by_rule"].values())


def test_success_finalization_clears_stale_error_summary(
    clean_db: Any,
    settings: Settings,
    project_root: Path,
) -> None:
    source = project_root / "data" / "sample_valid.csv"
    run = run_pipeline(source, run_id="it-clear-error", settings=settings, emit_metrics=False)
    assert run.status is RunStatus.SUCCESS

    with clean_db.cursor() as cursor:
        cursor.execute(
            "UPDATE observability.pipeline_runs "
            "SET status = 'FAILED', error_summary = 'transient database blip' "
            "WHERE run_id = %s",
            ("it-clear-error",),
        )
    clean_db.commit()

    finalized = finalize_run_stage("it-clear-error", settings=settings)

    assert finalized["status"] == "SUCCESS"
    assert finalized["error_summary"] is None


def test_corrupted_batch_quarantines_all_families_and_blocks_load(
    clean_db: Any,
    settings: Settings,
    project_root: Path,
) -> None:
    source = project_root / "data" / "sample_corrupted.csv"

    with pytest.raises(PermanentPipelineError, match="minimum is 10"):
        run_pipeline(source, run_id="it-corrupt-1", settings=settings, emit_metrics=False)

    run = _query_one(
        clean_db,
        "SELECT * FROM observability.pipeline_runs WHERE run_id = %s",
        ("it-corrupt-1",),
    )
    assert run["status"] == "FAILED"
    assert run["records_extracted"] == 9
    assert run["records_valid"] == 1
    assert run["records_rejected"] == 8
    assert run["records_loaded"] == 0
    assert float(run["rejection_rate"]) == pytest.approx(8 / 9, rel=1e-5)
    assert "minimum is 10" in run["error_summary"]
    assert run["failures_by_rule"]["row_count"] == 1
    for family in ("null", "duplicate", "schema", "range", "referential"):
        assert run["failures_by_rule"][family] > 0

    target = _query_one(clean_db, "SELECT COUNT(*) AS total FROM target.transactions")
    assert target["total"] == 0

    quarantine = _query_all(
        clean_db,
        "SELECT row_number, rule_families, failure_reasons "
        "FROM quality.error_records WHERE run_id = %s ORDER BY row_number",
        ("it-corrupt-1",),
    )
    assert len(quarantine) == 8
    observed = {family for row in quarantine for family in row["rule_families"]}
    assert observed == {"null", "duplicate", "schema", "range", "referential"}
    assert all(row["failure_reasons"] for row in quarantine)


def test_mixed_batch_loads_valid_rows_and_quarantines_only_bad_rows(
    clean_db: Any,
    settings: Settings,
    tmp_path: Path,
) -> None:
    rows = [_row(index) for index in range(1, 11)]
    rows.append(_row(1, amount="55.00"))
    rows.append(_row(12, account_id="ACC999999"))
    source = _write_csv(tmp_path / "mixed.csv", rows)

    run = run_pipeline(source, run_id="it-mixed-1", settings=settings, emit_metrics=False)

    assert run.status is RunStatus.SUCCESS
    assert run.records_extracted == 12
    assert run.records_loaded == 10
    assert run.records_rejected == 2
    assert run.rejection_rate == pytest.approx(2 / 12, rel=1e-5)
    assert run.rejection_rate > 0.10

    target = _query_one(clean_db, "SELECT COUNT(*) AS total FROM target.transactions")
    assert target["total"] == 10

    families = _query_all(
        clean_db,
        "SELECT rule_families FROM quality.error_records WHERE run_id = %s ORDER BY row_number",
        ("it-mixed-1",),
    )
    flattened = {family for row in families for family in row["rule_families"]}
    assert flattened == {"duplicate", "referential"}


def test_schema_corruption_blocks_whole_batch(
    clean_db: Any,
    settings: Settings,
    tmp_path: Path,
) -> None:
    rows = [_row(index) for index in range(30, 42)]
    source = _write_csv_without_amount(tmp_path / "schema_corrupt.csv", rows)

    with pytest.raises(PermanentPipelineError, match="Missing required column"):
        run_pipeline(source, run_id="it-schema-1", settings=settings, emit_metrics=False)

    run = _query_one(
        clean_db,
        "SELECT status, failures_by_rule, error_summary FROM observability.pipeline_runs "
        "WHERE run_id = %s",
        ("it-schema-1",),
    )
    assert run["status"] == "FAILED"
    assert run["failures_by_rule"]["schema"] == 13  # 12 row failures + 1 batch failure
    assert "Missing required column" in run["error_summary"]

    target = _query_one(clean_db, "SELECT COUNT(*) AS total FROM target.transactions")
    assert target["total"] == 0
    quarantine = _query_one(
        clean_db,
        "SELECT COUNT(*) AS total FROM quality.error_records WHERE run_id = %s",
        ("it-schema-1",),
    )
    assert quarantine["total"] == 12


def _write_csv_without_amount(path: Path, rows: list[dict[str, str]]) -> Path:
    columns = [
        "transaction_id",
        "account_id",
        "txn_type",
        "currency",
        "txn_date",
        "channel",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_missing_and_empty_files_fail_cleanly(
    clean_db: Any,
    settings: Settings,
    tmp_path: Path,
) -> None:
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(PermanentPipelineError, match="does not exist"):
        run_pipeline(missing, run_id="it-missing-1", settings=settings, emit_metrics=False)

    missing_run = _query_one(
        clean_db,
        "SELECT status, error_summary FROM observability.pipeline_runs WHERE run_id = %s",
        ("it-missing-1",),
    )
    assert missing_run["status"] == "FAILED"
    assert "does not exist" in missing_run["error_summary"]

    empty = tmp_path / "empty.csv"
    empty.write_text(
        "transaction_id,account_id,txn_type,amount,currency,txn_date,channel,status\n",
        encoding="utf-8",
    )
    with pytest.raises(PermanentPipelineError, match="minimum is 10"):
        run_pipeline(empty, run_id="it-empty-1", settings=settings, emit_metrics=False)

    empty_run = _query_one(
        clean_db,
        "SELECT status, records_extracted, error_summary "
        "FROM observability.pipeline_runs WHERE run_id = %s",
        ("it-empty-1",),
    )
    assert empty_run["status"] == "FAILED"
    assert empty_run["records_extracted"] == 0
    assert "minimum is 10" in empty_run["error_summary"]


def test_run_contract_round_trip(clean_db: Any, settings: Settings, project_root: Path) -> None:
    run = run_pipeline(
        project_root / "data" / "sample_valid.csv",
        run_id="it-contract-1",
        settings=settings,
        emit_metrics=False,
    )
    payload = json.loads(run.model_dump_json())
    assert payload["run_id"] == "it-contract-1"
    assert payload["status"] == "SUCCESS"
    assert payload["failures_by_rule"]["null"] == 0
