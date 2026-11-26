"""Airflow-independent pipeline stage service interface.

[DE] + [OR] Implement these stages.  [OR] owns begin_run_stage,
finalize_run_stage and mark_run_failed_stage (run audit logging, OR-06).
Each stage must persist enough state (run row, quarantine rows, staging JSON)
that a retry is safe and no dataframe ever crosses an XCom boundary.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from config.settings import DQConfig, Settings, get_settings
from etl.contracts import PermanentPipelineError, PipelineRun
from etl.load import fetch_pipeline_run, finish_pipeline_run, start_pipeline_run

logger = logging.getLogger(__name__)


def _truncate(value: str, limit: int = 2000) -> str:
    """Keep error summaries small enough for the audit table and dashboards."""

    return value if len(value) <= limit else value[: limit - 3] + "..."


def begin_run_stage(
    run_id: str,
    source_file: str,
    dag_id: str | None = None,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """[OR-06] Create the RUNNING audit row and return serializable run metadata."""

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

    Blocking batch violations (missing columns, row-count bounds) must persist
    progress and then raise a permanent pipeline error.
    """

    raise NotImplementedError("TODO [DE]: Implement this function")


def transform_and_load_stage(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Read staged records, transform them, and idempotently upsert the target."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def finalize_run_stage(
    run_id: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """[OR-06] Mark a run SUCCESS and return its final persisted contract."""

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
    """[OR-04/06] Best-effort failure finalization used by Airflow callbacks and the CLI.

    A database outage here is logged, not raised: re-raising would hide the
    original root-cause exception.
    """

    resolved = settings or get_settings()
    message = _truncate(str(error) or error.__class__.__name__)
    try:
        finish_pipeline_run(run_id, "FAILED", error_summary=message, settings=resolved)
    except Exception as exc:  # noqa: BLE001
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

    raise NotImplementedError("TODO [DE]: Implement this function")


def _build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for manual pipeline runs."""

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
    print(run.model_dump_json(indent=2))


if __name__ == "__main__":  # pragma: no cover - process entry point
    main()
