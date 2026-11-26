"""Thin Airflow DAG: orchestration wiring only, no dataframes and no SQL.

[OR] Owns OR-01..OR-06:

* OR-01/02  DAG + execution order: ``begin_run >> extract >> validate >> load >> emit_metrics >> done``
* OR-03     Retries: transient errors (``TransientPipelineError``, DB/network) use the
            exponential-backoff policy in ``default_args``.
* OR-04     Failed-task handling: ``PermanentPipelineError`` is translated into
            ``AirflowFailException`` (no retries) and ``_on_failure`` marks the run FAILED.
* OR-05     Dead-letter routing: the validate task delegates to the DE/DQ stage that
            writes rejected rows to ``quality.error_records`` *before* a blocking
            violation fails the run.
* OR-06     Execution logging: RUNNING row on ``begin_run``, SUCCESS after ``load``,
            FAILED from the failure callback (``observability.pipeline_runs``).

Business work lives in ``etl.pipeline`` / ``etl.extract`` / ``monitoring.metrics``.
Only small JSON-serializable metadata crosses XCom.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from config.settings import get_settings
from etl.contracts import PermanentPipelineError, TransientPipelineError
from etl.extract import extract_batch
from etl.pipeline import (
    begin_run_stage,
    extract_and_validate_stage,
    finalize_run_stage,
    mark_run_failed_stage,
    transform_and_load_stage,
)
from monitoring.metrics import emit_run_metrics

logger = logging.getLogger(__name__)
settings = get_settings()

T = TypeVar("T")
DAG_ID = settings.pipeline_dag_id
START_DATE = datetime(2024, 1, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fail_fast(task_callable: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a stage, converting terminal errors into non-retryable Airflow failures (OR-03/04)."""

    try:
        return task_callable(*args, **kwargs)
    except PermanentPipelineError as exc:
        logger.error("Permanent pipeline failure; retries suppressed: %s", exc)
        raise AirflowFailException(str(exc)) from exc


def _source_from_context(context: dict[str, Any]) -> str:
    """Allow ``{"source_file": "data/sample_corrupted.csv"}`` in the DAG run conf (needed for OR-07)."""

    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    configured = conf.get("source_file") if isinstance(conf, dict) else None
    return str(configured or settings.sample_valid_file)


def _run_metadata(context: dict[str, Any]) -> dict[str, Any]:
    """Read the small metadata dict that ``begin_run`` pushed to XCom."""

    metadata = context["ti"].xcom_pull(task_ids="begin_run") or {}
    if not metadata.get("run_id"):
        raise PermanentPipelineError("begin_run did not publish a run_id")
    return metadata


def _has_retry_attempt_left(context: dict[str, Any]) -> bool:
    """True when Airflow will retry this task instance again."""

    task_instance = context.get("task_instance")
    try:
        return int(task_instance.try_number) < int(task_instance.max_tries)
    except (AttributeError, TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def _begin_run(**context: Any) -> dict[str, Any]:
    """OR-06: insert the RUNNING audit row and publish {run_id, source_file, batch_id}."""

    return fail_fast(
        begin_run_stage,
        str(context["run_id"]),
        _source_from_context(context),
        DAG_ID,
        settings=settings,
    )


def _extract(**context: Any) -> dict[str, Any]:
    """Pre-flight read of the batch. Returns counts only, never the dataframe."""

    metadata = _run_metadata(context)
    result = extract_batch(metadata["source_file"])
    if result.fatal:
        # Missing/unreadable file will not fix itself on retry.
        raise AirflowFailException(f"Extraction failed ({result.error_kind}): {result.error}")
    return {
        "run_id": metadata["run_id"],
        "row_count": int(result.row_count),
        "is_empty": bool(result.is_empty),
    }


def _validate(**context: Any) -> dict[str, Any]:
    """OR-05: validate, route rejected rows to quality.error_records, stage valid rows.

    Blocking violations (missing columns, row-count bounds) raise
    PermanentPipelineError *after* quarantine is persisted -> AirflowFailException.
    """

    metadata = _run_metadata(context)
    return fail_fast(
        extract_and_validate_stage,
        str(metadata["run_id"]),
        str(metadata["source_file"]),
        settings=settings,
    )


def _load(**context: Any) -> dict[str, Any]:
    """Transform + idempotent upsert, then mark the run SUCCESS (OR-06)."""

    run_id = str(_run_metadata(context)["run_id"])
    load_summary = fail_fast(transform_and_load_stage, run_id, settings=settings)
    final_run = fail_fast(finalize_run_stage, run_id, settings=settings)
    return {"run_id": run_id, "load": load_summary, "status": final_run.get("status")}


def _emit_metrics(**context: Any) -> bool:
    """Best effort: the exporter also polls PostgreSQL, so this never fails the run."""

    try:
        run_id = str(_run_metadata(context)["run_id"])
        return bool(emit_run_metrics(run_id, settings=settings))
    except Exception as exc:  # noqa: BLE001 - telemetry must not gate the pipeline
        logger.warning("Metric refresh failed (non-blocking): %s", exc)
        return False


def _on_failure(context: dict[str, Any]) -> None:
    """OR-04: mark the run FAILED, unless a transient error still has retries left."""

    logger.error("Task failure callback invoked for %s", context.get("task_instance_key_str"))
    exception = context.get("exception")

    if isinstance(exception, TransientPipelineError) and _has_retry_attempt_left(context):
        logger.warning(
            "Transient failure on attempt %s/%s; leaving run RUNNING for Airflow to retry",
            getattr(context.get("task_instance"), "try_number", "?"),
            getattr(context.get("task_instance"), "max_tries", "?"),
        )
        return

    try:
        run_id = str(_run_metadata(context)["run_id"])
    except Exception:  # noqa: BLE001 - begin_run itself may have failed
        logger.warning("No run id available in failure context; skipping run finalization")
        return

    mark_run_failed_stage(run_id, exception or "Unknown task failure", settings=settings)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "obsiflow",
    "depends_on_past": False,
    "email_on_failure": False,
    # OR-03: 3 retries, 2 min -> 4 -> 8 ... capped at 30 min (transient errors only;
    # permanent ones are short-circuited by fail_fast/AirflowFailException).
    "retries": settings.pipeline_max_retries,
    "retry_delay": timedelta(seconds=settings.pipeline_retry_delay_seconds),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id=DAG_ID,
    description="Validate, quarantine, transform, and load a transaction batch.",
    schedule=settings.pipeline_schedule,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["dq", "etl", "observability", "daily"],
    params={"source_file": str(settings.sample_valid_file)},
    doc_md="""
# ObsiFlow DQ/ETL Pipeline

`begin_run >> extract >> validate >> load >> emit_metrics >> done`

(The former `extract_and_validate` step is split into explicit `extract` and
`validate` tasks so [DE] and [DQ] can work independently.)

1. **begin_run** - insert RUNNING row in `observability.pipeline_runs`.
2. **extract** - pre-flight read; missing/unreadable file fails permanently.
3. **validate** - six DQ rule families; rejected rows -> `quality.error_records`.
4. **load** - transform + idempotent upsert, then mark the run SUCCESS.
5. **emit_metrics** - best-effort exporter refresh (never gates the run).

Permanent failures raise `AirflowFailException` (no retries); transient
database/network errors retry with exponential backoff; the failure callback
marks the run FAILED. Override the batch with
`{"source_file": "data/sample_corrupted.csv"}` in the DAG run conf.
""",
) as dag:
    begin_run = PythonOperator(task_id="begin_run", python_callable=_begin_run)
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    validate = PythonOperator(task_id="validate", python_callable=_validate)
    load = PythonOperator(task_id="load", python_callable=_load)
    emit_metrics = PythonOperator(task_id="emit_metrics", python_callable=_emit_metrics)
    done = EmptyOperator(task_id="done")

    begin_run >> extract >> validate >> load >> emit_metrics >> done
