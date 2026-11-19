"""Airflow-independent pipeline stage service interface.

[DE] + [OR] Implement these stages.  Each stage must persist enough state (run
row, quarantine rows, staging JSON) that a retry is safe and no dataframe ever
crosses an XCom boundary.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from config.settings import DQConfig, Settings
from etl.contracts import PipelineRun

logger = logging.getLogger(__name__)


def begin_run_stage(
    run_id: str,
    source_file: str,
    dag_id: str | None = None,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Create the audit row and return serializable run metadata.

    TODO [DE]: insert the RUNNING row through ``etl.load.start_pipeline_run``.
    """

    raise NotImplementedError("TODO [DE]: Implement this function")


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
    """Mark a run SUCCESS and return its final persisted contract."""

    raise NotImplementedError("TODO [DE]: Implement this function")


def mark_run_failed_stage(
    run_id: str,
    error: BaseException | str,
    *,
    settings: Settings | None = None,
) -> None:
    """Best-effort failure finalization used by Airflow callbacks and the CLI."""

    raise NotImplementedError("TODO [DE]: Implement this function")


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
