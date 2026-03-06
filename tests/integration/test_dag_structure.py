"""Tier 2: DAG structure tests.

These tests verify that DAGs have the correct tasks, dependencies, and task groups
by querying the Airflow REST API.
"""

from __future__ import annotations

from typing import ClassVar

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.needs_airflow]

EXPECTED_DAG_IDS = {"simple_pipeline", "versioned_etl"}


class TestDagsLoaded:
    """Verify all expected DAGs are loaded."""

    def test_all_expected_dags_present(self, api_client: httpx.Client):
        resp = api_client.get("/api/v2/dags")
        assert resp.status_code == 200
        dag_ids = {d["dag_id"] for d in resp.json()["dags"]}
        missing = EXPECTED_DAG_IDS - dag_ids
        assert not missing, f"Missing DAGs: {missing}"

    def test_no_import_errors(self, api_client: httpx.Client):
        resp = api_client.get("/api/v2/importErrors")
        assert resp.status_code == 200
        errors = resp.json().get("import_errors", [])
        assert not errors, f"Import errors found: {errors}"


class TestSimplePipeline:
    """Verify the simple_pipeline DAG structure."""

    def test_has_expected_task(self, api_client: httpx.Client):
        resp = api_client.get("/api/v2/dags/simple_pipeline/tasks")
        assert resp.status_code == 200
        task_ids = {t["task_id"] for t in resp.json()["tasks"]}
        assert task_ids == {"process.clean"}

    def test_dag_has_tags(self, api_client: httpx.Client):
        resp = api_client.get("/api/v2/dags/simple_pipeline")
        assert resp.status_code == 200
        tags = {t["name"] for t in resp.json().get("tags", [])}
        assert "integration-test" in tags


class TestVersionedETL:
    """Verify the versioned_etl DAG structure with v1/v2 blueprints."""

    EXPECTED_TASKS: ClassVar[set[str]] = {
        "extract_legacy.validate",
        "extract_legacy.extract",
        "extract_multi.extract_orders",
        "extract_multi.extract_order_items",
        "transform.dedupe",
        "transform.normalize",
        "transform.enrich",
        "load_warehouse",
    }

    def _get_tasks(self, api_client: httpx.Client) -> dict:
        resp = api_client.get("/api/v2/dags/versioned_etl/tasks")
        assert resp.status_code == 200
        return {t["task_id"]: t for t in resp.json()["tasks"]}

    def test_all_expected_tasks_present(self, api_client: httpx.Client):
        tasks = self._get_tasks(api_client)
        assert set(tasks.keys()) == self.EXPECTED_TASKS

    def test_v1_extract_has_validate_and_extract(self, api_client: httpx.Client):
        """v1 Extract blueprint produces validate + extract tasks in a group."""
        tasks = self._get_tasks(api_client)
        v1_tasks = {t for t in tasks if t.startswith("extract_legacy.")}
        assert v1_tasks == {"extract_legacy.validate", "extract_legacy.extract"}

    def test_v2_extract_has_per_source_tasks(self, api_client: httpx.Client):
        """v2 Extract blueprint produces one task per source in a group."""
        tasks = self._get_tasks(api_client)
        v2_tasks = {t for t in tasks if t.startswith("extract_multi.")}
        assert v2_tasks == {"extract_multi.extract_orders", "extract_multi.extract_order_items"}

    def test_transform_has_chained_operations(self, api_client: httpx.Client):
        """Transform blueprint produces tasks chained in order."""
        tasks = self._get_tasks(api_client)
        assert tasks["transform.dedupe"]["downstream_task_ids"] == ["transform.normalize"]
        assert tasks["transform.normalize"]["downstream_task_ids"] == ["transform.enrich"]
        assert tasks["transform.enrich"]["downstream_task_ids"] == ["load_warehouse"]

    def test_extract_groups_feed_into_transform(self, api_client: httpx.Client):
        """Both extract step groups should have transform.dedupe as downstream."""
        tasks = self._get_tasks(api_client)
        extract_tasks = [t for t in tasks if t.startswith(("extract_legacy.", "extract_multi."))]
        for task_id in extract_tasks:
            assert "transform.dedupe" in tasks[task_id]["downstream_task_ids"], (
                f"{task_id} should flow into transform.dedupe"
            )

    def test_dag_has_tags(self, api_client: httpx.Client):
        resp = api_client.get("/api/v2/dags/versioned_etl")
        assert resp.status_code == 200
        tags = {t["name"] for t in resp.json().get("tags", [])}
        assert tags == {"integration-test", "etl"}
