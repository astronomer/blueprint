"""Tier 3: DAG execution tests.

These tests trigger actual DAG runs and verify they complete successfully.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from .conftest import AirflowAPI

pytestmark = [pytest.mark.integration, pytest.mark.needs_airflow]

DAG_RUN_TIMEOUT = 120
DAG_RUN_POLL_INTERVAL = 3
TERMINAL_STATES = {"success", "failed"}


def _unpause_dag(api_client: AirflowAPI, dag_id: str) -> None:
    resp = api_client.patch(f"/dags/{dag_id}", json={"is_paused": False})
    assert resp.status_code == 200, f"Failed to unpause {dag_id}: {resp.text}"


def _trigger_dag(api_client: AirflowAPI, dag_id: str) -> str:
    logical_date = datetime.now(tz=timezone.utc).isoformat()
    resp = api_client.post(
        f"/dags/{dag_id}/dagRuns",
        json={"logical_date": logical_date},
    )
    assert resp.status_code in {200, 201}, f"Failed to trigger {dag_id}: {resp.text}"
    return resp.json()["dag_run_id"]


def _wait_for_dag_run(api_client: AirflowAPI, dag_id: str, run_id: str) -> dict:
    deadline = time.monotonic() + DAG_RUN_TIMEOUT
    while time.monotonic() < deadline:
        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data.get("state") in TERMINAL_STATES:
            return data
        time.sleep(DAG_RUN_POLL_INTERVAL)

    msg = f"DAG run {dag_id}/{run_id} did not complete within {DAG_RUN_TIMEOUT}s"
    raise TimeoutError(msg)


class TestSimplePipelineExecution:
    def test_runs_successfully(self, api_client: AirflowAPI):
        dag_id = "simple_pipeline"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"


class TestVersionedETLExecution:
    def test_runs_successfully(self, api_client: AirflowAPI):
        dag_id = "versioned_etl"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"

    def test_all_tasks_succeed(self, api_client: AirflowAPI):
        dag_id = "versioned_etl"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success"

        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"


class TestDagArgsExecution:
    def test_runs_successfully(self, api_client: AirflowAPI):
        dag_id = "dag_args_test"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"

    def test_all_tasks_succeed(self, api_client: AirflowAPI):
        dag_id = "dag_args_test"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success"

        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"
