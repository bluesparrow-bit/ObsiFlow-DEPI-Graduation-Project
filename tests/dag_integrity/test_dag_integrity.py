"""DagBag integrity checks: import errors, cycles, retries, tags, thinness."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
DAG_ID = "dq_etl_pipeline"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.mark.dag_integrity
def test_dagbag_has_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, dagbag.import_errors


@pytest.mark.dag_integrity
def test_expected_dag_is_registered(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    assert dag is not None
    assert dag.dag_id == DAG_ID
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule_interval == "0 2 * * *"


@pytest.mark.dag_integrity
def test_dag_has_required_tags_and_documentation(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    assert {"dq", "etl", "observability"}.issubset(set(dag.tags))
    assert dag.doc_md and "extract_and_validate" in dag.doc_md


@pytest.mark.dag_integrity
def test_task_graph_is_acyclic_and_complete(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    expected_tasks = {
        "begin_run",
        "extract_and_validate",
        "transform_and_load",
        "finalize_run",
        "emit_metrics",
        "done",
    }
    assert set(dag.task_ids) == expected_tasks
    ordered = dag.topological_sort()
    assert next(task.task_id for task in ordered) == "begin_run"
    assert [task.task_id for task in ordered][-1] == "done"


@pytest.mark.dag_integrity
def test_retry_policy_and_failure_callback(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    assert dag.default_args["retries"] >= 2
    assert dag.default_args["retry_exponential_backoff"] is True
    assert callable(dag.default_args["on_failure_callback"])
    for task in dag.tasks:
        assert task.retries == dag.default_args["retries"]
        assert task.on_failure_callback is not None


@pytest.mark.dag_integrity
def test_dag_module_stays_thin() -> None:
    source = (DAGS_DIR / "dq_etl_pipeline_dag.py").read_text(encoding="utf-8")
    for forbidden in ("import pandas", "import psycopg2", "validate_batch(", "execute_values("):
        assert forbidden not in source
