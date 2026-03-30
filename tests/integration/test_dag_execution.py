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


def _trigger_dag(
    api_client: AirflowAPI,
    dag_id: str,
    conf: dict | None = None,
) -> str:
    logical_date = datetime.now(tz=timezone.utc).isoformat()
    body: dict = {"logical_date": logical_date}
    if conf is not None:
        body["conf"] = conf
    resp = api_client.post(
        f"/dags/{dag_id}/dagRuns",
        json=body,
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


class TestExplicitNamingExecution:
    """Execute the explicit_naming DAG using blueprints with explicit name/version."""

    def test_runs_successfully(self, api_client: AirflowAPI):
        dag_id = "explicit_naming"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"

    def test_all_tasks_succeed(self, api_client: AirflowAPI):
        dag_id = "explicit_naming"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success"

        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"


class TestContextProxyExecution:
    """Execute the context_test DAG using {{ context.* }} expressions in YAML.

    Verifies that context proxy values (e.g. {{ context.ds_nodash }}) pass through
    as literal Airflow template strings and are rendered to real values at task
    execution time (not left as raw {{ ds_nodash }} literals).
    """

    DAG_ID = "context_test"

    def _run_dag(self, api_client: AirflowAPI) -> str:
        _unpause_dag(api_client, self.DAG_ID)
        run_id = _trigger_dag(api_client, self.DAG_ID)
        result = _wait_for_dag_run(api_client, self.DAG_ID, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"
        return run_id

    def _get_rendered_fields(self, api_client: AirflowAPI, run_id: str, task_id: str) -> dict:
        resp = api_client.get(f"/dags/{self.DAG_ID}/dagRuns/{run_id}/taskInstances/{task_id}")
        assert resp.status_code == 200, resp.text
        return resp.json().get("rendered_fields", {})

    def test_all_tasks_succeed(self, api_client: AirflowAPI):
        run_id = self._run_dag(api_client)

        resp = api_client.get(f"/dags/{self.DAG_ID}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"

    def test_ds_nodash_rendered(self, api_client: AirflowAPI):
        """Verify {{ context.ds_nodash }} was rendered to a real YYYYMMDD date."""
        run_id = self._run_dag(api_client)
        rendered = self._get_rendered_fields(api_client, run_id, "ctx.echo_partition")
        bash_cmd = rendered.get("bash_command", "")
        assert "partition=" in bash_cmd, f"Unexpected bash_command: {bash_cmd}"
        partition_value = bash_cmd.split("partition=")[1].strip().rstrip("'")
        assert partition_value.isdigit() and len(partition_value) == 8, (
            f"Expected YYYYMMDD date, got {partition_value!r} in: {bash_cmd}"
        )

    def test_ds_rendered_in_path(self, api_client: AirflowAPI):
        """Verify {{ context.ds }} was rendered to a YYYY-MM-DD date inside the path."""
        run_id = self._run_dag(api_client)
        rendered = self._get_rendered_fields(api_client, run_id, "ctx.echo_path")
        bash_cmd = rendered.get("bash_command", "")
        assert "s3://bucket/" in bash_cmd, f"Unexpected bash_command: {bash_cmd}"
        assert "/data.parquet" in bash_cmd, f"Unexpected bash_command: {bash_cmd}"
        # Extract the date between s3://bucket/ and /data.parquet
        path_part = bash_cmd.split("s3://bucket/")[1].split("/data.parquet")[0]
        parts = path_part.split("-")
        assert len(parts) == 3 and all(p.isdigit() for p in parts), (
            f"Expected YYYY-MM-DD date in path, got {path_part!r} in: {bash_cmd}"
        )


class TestParamsExecution:
    """Execute the params_test DAG with default and overridden params.

    Validates both param access patterns end-to-end:
    - Template access (BashOperator with {{ params.x }})
    - Variable access (resolve_config inside @task)
    """

    def test_default_params_execution(self, api_client: AirflowAPI):
        dag_id = "params_test"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"

        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"

    def test_overridden_params_execution(self, api_client: AirflowAPI):
        dag_id = "params_test"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(
            api_client,
            dag_id,
            conf={"greet__message": "overridden-hello", "greet__repeat": 3},
        )
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", f"DAG run state: {result['state']}"

        resp = api_client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        assert resp.status_code == 200
        task_instances = resp.json().get("task_instances", [])
        failed = [ti["task_id"] for ti in task_instances if ti.get("state") != "success"]
        assert not failed, f"Tasks did not succeed: {failed}"
