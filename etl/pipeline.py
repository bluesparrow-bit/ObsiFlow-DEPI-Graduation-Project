"""Thin, Airflow-independent pipeline service.

The DAG only wires these stage functions together.  Each stage persists enough
state (run row, quarantine rows, staging JSON) that a retry is safe and no
dataframe ever crosses an XCom boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import DQConfig, Settings, get_settings, load_dq_config
from etl.contracts import (
    PermanentPipelineError,
    PipelineRun,
    RunCounters,
    TransactionRecord,
)
from etl.extract import extract_batch
from etl.load import (
    fetch_pipeline_run,
    fetch_reference_account_ids,
    finish_pipeline_run,
    load_transactions,
    start_pipeline_run,
    update_run_loaded,
    update_run_progress,
)
from etl.transform import transform_records
from monitoring.metrics import emit_run_metrics
from validation.engine import validate_batch
from validation.quarantine import persist_rejected_rows

logger = logging.getLogger(__name__)

VALID_RECORDS_FILE = "valid_records.json"
RUN_SUMMARY_FILE = "run_summary.json"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


def _run_directory(run_id: str, settings: Settings) -> Path:
    safe = _SAFE_NAME.sub("_", run_id)[:80]
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:10]
    directory = settings.staging_dir / f"{safe}-{digest}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise PermanentPipelineError(f"Required staging artefact is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _truncate(value: str, limit: int = 2000) -> str:
    return value if len(value) <= limit else value[: limit - 3] + "..."


def begin_run_stage(
    run_id: str,
    source_file: str,
    dag_id: str | None = None,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Create the audit row and return serializable run metadata."""

    resolved = settings or get_settings()
    resolved.ensure_runtime_directories()
    resolved_dag_id = dag_id or resolved.pipeline_dag_id
    source = Path(source_file)
    batch_id = source.stem
    start_pipeline_run(
        run_id=run_id,
        dag_id=resolved_dag_id,
        source_file=str(source),
        batch_id=batch_id,
        settings=resolved,
    )
    return {
        "run_id": run_id,
        "dag_id": resolved_dag_id,
        "source_file": str(source),
        "batch_id": batch_id,
    }


def extract_and_validate_stage(
    run_id: str,
    source_file: str,
    *,
    settings: Settings | None = None,
    config: DQConfig | None = None,
) -> dict[str, Any]:
    """Extract, validate, quarantine, and stage valid records.

    Blocking batch violations (missing columns, row-count bounds) raise
    :class:`PermanentPipelineError` after quarantine and progress are persisted,
    so the run can be diagnosed without retrying a doomed batch.
    """

    resolved = settings or get_settings()
    resolved.ensure_runtime_directories()
    dq_config = config or load_dq_config(resolved.dq_rules_path)
    counters = RunCounters()
    extraction = extract_batch(source_file)

    if extraction.fatal:
        message = extraction.error or f"Fatal extraction failure: {source_file}"
        update_run_progress(
            run_id,
            counters.snapshot(),
            {},
            error_summary=_truncate(message),
            settings=resolved,
        )
        raise PermanentPipelineError(message)

    counters.set_extracted(extraction.row_count)

    if extraction.row_count:
        reference_ids = fetch_reference_account_ids(dq_config, settings=resolved)
    else:
        reference_ids = set()

    result = validate_batch(
        extraction.frame,
        dq_config,
        reference_ids,
        source_file=str(source_file),
    )
    counters.add_valid(len(result.valid_records))
    counters.add_rejected(len(result.rejected_rows))

    persist_rejected_rows(
        run_id,
        str(source_file),
        result.rejected_rows,
        config=dq_config,
        settings=resolved,
    )

    blocking_message = "; ".join(
        violation.message for violation in result.batch_violations if violation.blocking
    )
    update_run_progress(
        run_id,
        counters.snapshot(),
        result.failures_by_family,
        error_summary=_truncate(blocking_message) if blocking_message else None,
        settings=resolved,
    )

    directory = _run_directory(run_id, resolved)
    valid_path = directory / VALID_RECORDS_FILE
    _write_json(valid_path, [record.model_dump(mode="json") for record in result.valid_records])

    summary: dict[str, Any] = {
        "run_id": run_id,
        "source_file": str(source_file),
        "total_rows": result.total_rows,
        "valid_rows": len(result.valid_records),
        "rejected_rows": len(result.rejected_rows),
        "rejection_rate": result.rejection_rate,
        "rejection_threshold_exceeded": result.rejection_threshold_exceeded,
        "is_blocking": result.is_blocking,
        "failures_by_family": result.failures_by_family,
        "batch_violations": [
            violation.model_dump(mode="json") for violation in result.batch_violations
        ],
        "valid_records_file": str(valid_path),
    }
    _write_json(directory / RUN_SUMMARY_FILE, summary)

    if result.is_blocking:
        raise PermanentPipelineError(
            blocking_message or "Batch failed a blocking data-quality rule"
        )

    return summary


def transform_and_load_stage(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Read staged records, transform them, and idempotently upsert the target."""

    resolved = settings or get_settings()
    directory = _run_directory(run_id, resolved)
    summary = _read_json(directory / RUN_SUMMARY_FILE)
    payload = _read_json(directory / VALID_RECORDS_FILE)

    records = [TransactionRecord.model_validate(item) for item in payload]
    transformed = transform_records(records)
    loaded = load_transactions(
        transformed,
        run_id=run_id,
        source_file=str(summary["source_file"]),
        settings=resolved,
    )
    update_run_loaded(run_id, loaded, settings=resolved)
    return {"run_id": run_id, "loaded": loaded}


def finalize_run_stage(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Mark a run SUCCESS and return its final persisted contract."""

    resolved = settings or get_settings()
    finish_pipeline_run(run_id, "SUCCESS", settings=resolved)
    run = fetch_pipeline_run(run_id, settings=resolved)
    if run is None:
        raise PermanentPipelineError(f"Run disappeared during finalization: {run_id}")
    return run.model_dump(mode="json")


def mark_run_failed_stage(
    run_id: str,
    error: BaseException | str,
    *,
    settings: Settings | None = None,
) -> None:
    """Best-effort failure finalization used by Airflow callbacks and the CLI.

    A database outage during failure handling is logged, not raised: losing the
    original exception would hide the real root cause.
    """

    resolved = settings or get_settings()
    message = _truncate(str(error) or error.__class__.__name__)
    try:
        finish_pipeline_run(run_id, "FAILED", error_summary=message, settings=resolved)
    except Exception as exc:
        logger.error("Could not mark run %s as FAILED: %s", run_id, exc)


def run_pipeline(
    source_file: str | Path,
    run_id: str | None = None,
    dag_id: str | None = None,
    *,
    settings: Settings | None = None,
    config: DQConfig | None = None,
    emit_metrics: bool = True,
) -> PipelineRun:
    """Execute all stages synchronously (CLI, tests, ad-hoc backfills)."""

    resolved = settings or get_settings()
    resolved.ensure_runtime_directories()
    resolved_run_id = run_id or f"manual__{uuid4().hex}"
    resolved_dag_id = dag_id or resolved.pipeline_dag_id
    source = str(source_file)

    begin_run_stage(resolved_run_id, source, resolved_dag_id, settings=resolved)
    try:
        extract_and_validate_stage(
            resolved_run_id,
            source,
            settings=resolved,
            config=config,
        )
        transform_and_load_stage(resolved_run_id, settings=resolved)
        finalize_run_stage(resolved_run_id, settings=resolved)
    except Exception as exc:
        mark_run_failed_stage(resolved_run_id, exc, settings=resolved)
        raise
    finally:
        if emit_metrics:
            emit_run_metrics(resolved_run_id, settings=resolved)

    completed = fetch_pipeline_run(resolved_run_id, settings=resolved)
    if completed is None:
        raise PermanentPipelineError(f"Run disappeared after completion: {resolved_run_id}")
    return completed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the ObsiFlow DQ/ETL pipeline once.")
    parser.add_argument("--source", required=True, help="CSV batch to process")
    parser.add_argument("--run-id", default=None, help="Explicit run id (defaults to a UUID)")
    parser.add_argument("--dag-id", default=None, help="Logical DAG id for telemetry")
    parser.add_argument("--no-metrics", action="store_true", help="Skip exporter refresh")
    return parser


def main() -> None:
    """CLI entry point: ``python -m etl.pipeline --source data/sample_valid.csv``."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    run = run_pipeline(
        args.source,
        run_id=args.run_id,
        dag_id=args.dag_id,
        emit_metrics=not args.no_metrics,
    )
    print(json.dumps(run.model_dump(mode="json"), indent=2))


if __name__ == "__main__":  # pragma: no cover - process entry point
    main()
