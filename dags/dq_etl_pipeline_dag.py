"""Thin Airflow DAG: context wiring only, no dataframes and no SQL.

All heavy work lives in ``etl.pipeline``.  Permanent validation failures are
converted to ``AirflowFailException`` so Airflow skips pointless retries, while
transient database/network errors propagate and use the configured retry policy.
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


def fail_fast(task_callable: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a stage, converting terminal errors into non-retryable Airflow failures."""

    try:
        return task_callable(*args, **kwargs)
    except PermanentPipelineError as exc:
        logger.error("Permanent pipeline failure; retries suppressed: %s", exc)
        raise AirflowFailException(str(exc)) from exc


def _source_from_context(context: dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    configured = conf.get("source_file") if isinstance(conf, dict) else None
    return str(configured or settings.sample_valid_file)


def _run_id_from_xcom(context: dict[str, Any]) -> str:
    metadata = context["ti"].xcom_pull(task_ids="begin_run") or {}
    return str(metadata["run_id"])


def _begin_run(**context: dict[str, Any]) -> dict[str, Any]:
    return fail_fast(
        begin_run_stage,
        str(context["run_id"]),
        _source_from_context(context),
        settings=settings,
    )


def _extract_and_validate(**context: dict[str, Any]) -> dict[str, Any]:
    metadata = context["ti"].xcom_pull(task_ids="begin_run") or {}
    return fail_fast(
        extract_and_validate_stage,
        str(metadata["run_id"]),
        str(metadata["source_file"]),
        settings=settings,
    )


def _transform_and_load(**context: dict[str, Any]) -> dict[str, Any]:
    return fail_fast(
        transform_and_load_stage,
        _run_id_from_xcom(context),
        settings=settings,
    )


def _finalize(**context: dict[str, Any]) -> dict[str, Any]:
    return fail_fast(
        finalize_run_stage,
        _run_id_from_xcom(context),
        settings=settings,
    )


def _emit_metrics(**context: dict[str, Any]) -> bool:
    return emit_run_metrics(_run_id_from_xcom(context), settings=settings)


def _has_retry_attempt_left(context: dict[str, Any]) -> bool:
    """Return True when Airflow will retry the failed task instance."""

    task_instance = context.get("task_instance")
    try:
        return int(task_instance.try_number) < int(task_instance.max_tries)
    except (AttributeError, TypeError, ValueError):
        return False


def _on_failure(context: dict[str, Any]) -> None:
    logger.error("Task failure callback invoked for %s", context.get("task_instance_key_str"))
    exception = context.get("exception")
    if isinstance(exception, TransientPipelineError) and _has_retry_attempt_left(context):
        logger.warning(
            "Transient failure on attempt %s/%s; leaving the run RUNNING for Airflow to retry",
            getattr(context.get("task_instance"), "try_number", "?"),
            getattr(context.get("task_instance"), "max_tries", "?"),
        )
        return
    try:
        run_id = _run_id_from_xcom(context)
    except Exception:
        logger.warning("No run id available in failure context; skipping run finalization")
        return
    mark_run_failed_stage(run_id, exception or "Unknown task failure", settings=settings)


default_args = {
    "owner": "obsiflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": settings.pipeline_max_retries,
    "retry_delay": timedelta(seconds=settings.pipeline_retry_delay_seconds),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
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

Thin orchestration wrapper around `etl.pipeline`:

1. **begin_run** — insert the `observability.pipeline_runs` audit row.
2. **extract_and_validate** — extract CSV, run all six DQ rule families, persist
   quarantine rows, stage valid records as JSON.
3. **transform_and_load** — normalize and idempotently upsert valid rows.
4. **finalize_run** — mark the run `SUCCESS` with duration and counters.
5. **emit_metrics** — best-effort in-task telemetry refresh. The standalone
   exporter polls PostgreSQL independently, so this step never gates the run.

Permanent validation failures raise `AirflowFailException` (no retries).
Transient database/network errors use exponential-backoff retries. Override the
batch per run with `{"source_file": "data/sample_corrupted.csv"}` in DAG run conf.
""",
) as dag:
    begin_run = PythonOperator(
        task_id="begin_run",
        python_callable=_begin_run,
    )
    extract_and_validate = PythonOperator(
        task_id="extract_and_validate",
        python_callable=_extract_and_validate,
    )
    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=_transform_and_load,
    )
    finalize_run = PythonOperator(
        task_id="finalize_run",
        python_callable=_finalize,
    )
    emit_metrics = PythonOperator(
        task_id="emit_metrics",
        python_callable=_emit_metrics,
    )
    done = EmptyOperator(task_id="done")

    (
        begin_run
        >> extract_and_validate
        >> transform_and_load
        >> finalize_run
        >> emit_metrics
        >> done
    )
