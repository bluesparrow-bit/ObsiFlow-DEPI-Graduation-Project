"""Failure-policy tests: transient retries vs non-retryable Airflow failures."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import TypeVar
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("airflow")

from airflow.exceptions import AirflowFailException

from dags.dq_etl_pipeline_dag import _on_failure, fail_fast
from etl.contracts import PermanentPipelineError, TransientPipelineError

pytestmark = [pytest.mark.integration]

T = TypeVar("T")


def _retry_loop(action: Callable[[], T], retries: int) -> tuple[T, int]:
    """Mimic Airflow's retry semantics for the unit-level policy assertion."""

    for attempt in range(1, retries + 2):
        try:
            return action(), attempt
        except AirflowFailException:
            raise
        except TransientPipelineError:
            if attempt > retries:
                raise
    raise AssertionError("unreachable")


def test_permanent_failure_is_converted_to_airflow_fail_exception() -> None:
    action = Mock(side_effect=PermanentPipelineError("missing required column"))

    with pytest.raises(AirflowFailException, match="missing required column"):
        fail_fast(action)

    assert action.call_count == 1


def test_permanent_failure_is_not_retried() -> None:
    action = Mock(side_effect=PermanentPipelineError("invalid contract"))
    with pytest.raises(AirflowFailException):
        _retry_loop(lambda: fail_fast(action), retries=3)
    assert action.call_count == 1


def test_transient_failure_is_retried_until_success() -> None:
    action = Mock(side_effect=[TransientPipelineError("connection reset"), "loaded"])

    result, attempts = _retry_loop(lambda: fail_fast(action), retries=3)

    assert result == "loaded"
    assert attempts == 2
    assert action.call_count == 2


def test_transient_failure_exhausts_retries_and_propagates() -> None:
    action = Mock(side_effect=TransientPipelineError("database restarting"))

    with pytest.raises(TransientPipelineError):
        _retry_loop(lambda: fail_fast(action), retries=2)

    assert action.call_count == 3


def test_failure_callback_marks_run_failed() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "exception": ValueError("boom"),
        "task_instance_key_str": "dq_etl_pipeline.extract_and_validate",
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[0] == "run-123"


def test_failure_callback_skips_finalization_when_no_run_id() -> None:
    xcom = Mock(side_effect=RuntimeError("no xcom"))
    context = {"ti": SimpleNamespace(xcom_pull=xcom), "exception": ValueError("boom")}

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_not_called()


def test_failure_callback_defers_transient_failure_while_retries_remain() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "task_instance": SimpleNamespace(try_number=1, max_tries=4),
        "exception": TransientPipelineError("connection reset"),
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_not_called()


def test_failure_callback_marks_transient_failure_on_last_attempt() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "task_instance": SimpleNamespace(try_number=4, max_tries=4),
        "exception": TransientPipelineError("database restarting"),
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_called_once()
    assert "database restarting" in str(mark_failed.call_args.args[1])


def test_failure_callback_defers_unclassified_failure_while_retries_remain() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "task_instance": SimpleNamespace(try_number=1, max_tries=4),
        "exception": ValueError("unexpected bug"),
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_not_called()


def test_failure_callback_marks_unclassified_failure_on_last_attempt() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "task_instance": SimpleNamespace(try_number=4, max_tries=4),
        "exception": ValueError("unexpected bug"),
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_called_once()
    assert "unexpected bug" in str(mark_failed.call_args.args[1])


def test_failure_callback_marks_permanent_failure_immediately() -> None:
    xcom = Mock(return_value={"run_id": "run-123"})
    context = {
        "ti": SimpleNamespace(xcom_pull=xcom),
        "task_instance": SimpleNamespace(try_number=1, max_tries=4),
        "exception": AirflowFailException("missing required column"),
    }

    with patch("dags.dq_etl_pipeline_dag.mark_run_failed_stage") as mark_failed:
        _on_failure(context)

    mark_failed.assert_called_once()
    assert "missing required column" in str(mark_failed.call_args.args[1])
