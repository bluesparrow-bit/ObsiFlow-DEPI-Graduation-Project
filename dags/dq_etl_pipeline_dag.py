"""Thin Airflow DAG skeleton: wiring only, no dataframes and no SQL.

[OR] Keep orchestration concerns here and delegate business work to the role
owners.  The logical chain is ``extract >> validate >> load >> emit_metrics``
wrapped by audit/finalization tasks.

TODO [OR]:
* Persist RUNNING/SUCCESS/FAILED rows in ``observability.pipeline_runs``.
* Decide retry policy per task and translate permanent failures with
  ``AirflowFailException`` so doomed batches are not retried.
* Pass only small, serializable metadata through XCom; never dataframes.
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
from etl.contracts import PermanentPipelineError
from etl.extract import extract_batch
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


def _begin_run(**context: dict[str, Any]) -> dict[str, Any]:
    """Create the run audit row before any business stage executes."""

    # TODO [OR]: Insert a RUNNING row and return {"run_id", "source_file"}.
    raise NotImplementedError("TODO [OR]: Implement DB run logging")


def _extract(**context: dict[str, Any]) -> Any:
    """Read the batch as raw strings via the DE extraction interface."""

    return extract_batch(settings.sample_valid_file)


def _validate(**context: dict[str, Any]) -> dict[str, Any]:
    """Run the six DQ rule families and stage the valid/invalid split."""

    # TODO [DQ]: Call validation.engine.validate_batch and persist quarantine.
    raise NotImplementedError("TODO [DQ]: Implement the validation stage")


def _load(**context: dict[str, Any]) -> dict[str, Any]:
    """Transform staged records and idempotently upsert them into PostgreSQL."""

    # TODO [DE]: Read staging artefacts, transform, and call etl.load.load_transactions.
    raise NotImplementedError("TODO [DE]: Implement the load stage")


def _emit_metrics(**context: dict[str, Any]) -> bool:
    """Ask the observability exporter to refresh its DB-backed snapshot."""

    return emit_run_metrics(str(context.get("run_id") or ""), settings=settings)


def _on_failure(context: dict[str, Any]) -> None:
    """Failure callback placeholder for audit finalization."""

    logger.error("Task failure callback invoked for %s", context.get("task_instance_key_str"))
    # TODO [OR]: Mark the run FAILED unless a retry attempt remains.
    logger.warning("TODO [OR]: pipeline_runs failure finalization is not implemented yet")


with DAG(
    dag_id=DAG_ID,
    description="Validate, quarantine, transform, and load a transaction batch.",
    schedule=settings.pipeline_schedule,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "obsiflow",
        "depends_on_past": False,
        "email_on_failure": False,
        # TODO [OR]: Tune retries/backoff per failure class.
        "retries": settings.pipeline_max_retries,
        "retry_delay": timedelta(seconds=settings.pipeline_retry_delay_seconds),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": _on_failure,
    },
    tags=["dq", "etl", "observability", "daily"],
    params={"source_file": str(settings.sample_valid_file)},
    doc_md="""
# ObsiFlow DQ/ETL Pipeline (Skeleton)

Logical chain:

`begin_run >> extract >> validate >> load >> emit_metrics >> done`

The former `extract_and_validate` step is split into explicit `extract` and
`validate` tasks so [DE] and [DQ] can work independently.

- [OR] owns task wiring, retries, failure callbacks, and run audit logging.
- [DE] owns extraction, transformation, and loading.
- [DQ] owns validation and quarantine.
- [OB] owns exporter refresh and alerting.

Permanent validation failures should raise `AirflowFailException` (no retries);
transient database/network errors should use exponential-backoff retries.
Override the batch per run with `{"source_file": "data/sample_corrupted.csv"}`.
""",
) as dag:
    begin_run = PythonOperator(task_id="begin_run", python_callable=_begin_run)
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    validate = PythonOperator(task_id="validate", python_callable=_validate)
    load = PythonOperator(task_id="load", python_callable=_load)
    emit_metrics = PythonOperator(task_id="emit_metrics", python_callable=_emit_metrics)
    done = EmptyOperator(task_id="done")

    begin_run >> extract >> validate >> load >> emit_metrics >> done
