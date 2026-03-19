"""Tier 2: DAG structure tests.

These tests verify that DAGs have the correct tasks, dependencies, and task groups
by querying the Airflow REST API. Also validates that the BlueprintDagArgs team/tier
abstraction renders the expected Airflow DAG kwargs.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pytest

if TYPE_CHECKING:
    from .conftest import AirflowAPI

pytestmark = [pytest.mark.integration, pytest.mark.needs_airflow]

EXPECTED_DAG_IDS = {"simple_pipeline", "versioned_etl", "dag_args_test", "explicit_naming"}


class TestDagsLoaded:
    def test_all_expected_dags_present(self, api_client: AirflowAPI):
        dag_ids = api_client.get_dag_ids()
        missing = EXPECTED_DAG_IDS - dag_ids
        assert not missing, f"Missing DAGs: {missing}"

    def test_no_import_errors(self, api_client: AirflowAPI):
        resp = api_client.get("/importErrors")
        assert resp.status_code == 200
        errors = resp.json().get("import_errors", [])
        assert not errors, f"Import errors found: {errors}"


class TestSimplePipeline:
    """Verify the simple_pipeline DAG structure.

    simple.dag.yaml sets team=data-eng but uses default tier (standard).
    Expected derived values:
      tags: [team:data-eng, standard]
      owner: data-eng
      retries: 1  (standard tier)
    """

    def test_has_expected_task(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline/tasks")
        assert resp.status_code == 200
        task_ids = {t["task_id"] for t in resp.json()["tasks"]}
        assert task_ids == {"process.clean"}

    def test_team_tag_generated(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline")
        assert resp.status_code == 200
        tags = api_client.get_tags(resp.json())
        assert "team:data-eng" in tags

    def test_tier_tag_generated(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline")
        assert resp.status_code == 200
        tags = api_client.get_tags(resp.json())
        assert "standard" in tags

    def test_owner_derived_from_team(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline/tasks")
        assert resp.status_code == 200
        for task in resp.json()["tasks"]:
            assert task.get("owner") == "data-eng"

    def test_standard_tier_retries(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline/tasks")
        assert resp.status_code == 200
        for task in resp.json()["tasks"]:
            assert task.get("retries") == 1, (
                f"Task {task['task_id']} retries={task.get('retries')}, expected 1 (standard tier)"
            )


class TestVersionedETL:
    """Verify the versioned_etl DAG structure with v1/v2 blueprints.

    versioned.dag.yaml sets team=data-eng, tier=critical.
    """

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

    def _get_tasks(self, api_client: AirflowAPI) -> dict:
        resp = api_client.get("/dags/versioned_etl/tasks")
        assert resp.status_code == 200
        return {t["task_id"]: t for t in resp.json()["tasks"]}

    def test_all_expected_tasks_present(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert set(tasks.keys()) == self.EXPECTED_TASKS

    def test_v1_extract_has_validate_and_extract(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        v1_tasks = {t for t in tasks if t.startswith("extract_legacy.")}
        assert v1_tasks == {"extract_legacy.validate", "extract_legacy.extract"}

    def test_v2_extract_has_per_source_tasks(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        v2_tasks = {t for t in tasks if t.startswith("extract_multi.")}
        assert v2_tasks == {"extract_multi.extract_orders", "extract_multi.extract_order_items"}

    def test_transform_has_chained_operations(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert tasks["transform.dedupe"]["downstream_task_ids"] == ["transform.normalize"]
        assert tasks["transform.normalize"]["downstream_task_ids"] == ["transform.enrich"]
        assert tasks["transform.enrich"]["downstream_task_ids"] == ["load_warehouse"]

    def test_extract_groups_feed_into_transform(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        extract_tasks = [t for t in tasks if t.startswith(("extract_legacy.", "extract_multi."))]
        for task_id in extract_tasks:
            assert "transform.dedupe" in tasks[task_id]["downstream_task_ids"], (
                f"{task_id} should flow into transform.dedupe"
            )

    def test_dag_has_tags(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/versioned_etl")
        assert resp.status_code == 200
        tags = api_client.get_tags(resp.json())
        assert tags == {"team:data-eng", "critical"}

    def test_critical_tier_retries(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        for task_id, task in tasks.items():
            assert task.get("retries") == 3, (
                f"Task {task_id} retries={task.get('retries')}, expected 3 (critical tier)"
            )


class TestExplicitNaming:
    """Verify the explicit_naming DAG using blueprints with explicit name/version attrs.

    Covers: explicit name+version (ingest v1/v2), explicit name only (quality_check),
    and explicit version only (notify).
    """

    EXPECTED_TASKS: ClassVar[set[str]] = {
        "ingest_v1.download",
        "ingest_v1.parse",
        "ingest_v2.stream_events",
        "ingest_v2.stream_clicks",
        "check.not_null",
        "check.unique",
        "notify",
    }

    def _get_tasks(self, api_client: AirflowAPI) -> dict:
        resp = api_client.get("/dags/explicit_naming/tasks")
        assert resp.status_code == 200
        return {t["task_id"]: t for t in resp.json()["tasks"]}

    def test_all_expected_tasks_present(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert set(tasks.keys()) == self.EXPECTED_TASKS

    def test_ingest_v1_uses_explicit_name(self, api_client: AirflowAPI):
        """S3DataIngester (name='ingest', version=1) produces download+parse."""
        tasks = self._get_tasks(api_client)
        v1_tasks = {t for t in tasks if t.startswith("ingest_v1.")}
        assert v1_tasks == {"ingest_v1.download", "ingest_v1.parse"}

    def test_ingest_v2_uses_explicit_name(self, api_client: AirflowAPI):
        """StreamingIngester (name='ingest', version=2) produces per-source tasks."""
        tasks = self._get_tasks(api_client)
        v2_tasks = {t for t in tasks if t.startswith("ingest_v2.")}
        assert v2_tasks == {"ingest_v2.stream_events", "ingest_v2.stream_clicks"}

    def test_quality_check_explicit_name_only(self, api_client: AirflowAPI):
        """DataQualityValidator (name='quality_check', version inferred) produces check tasks."""
        tasks = self._get_tasks(api_client)
        qc_tasks = {t for t in tasks if t.startswith("check.")}
        assert qc_tasks == {"check.not_null", "check.unique"}

    def test_notify_explicit_version_only(self, api_client: AirflowAPI):
        """Notify (version=1 explicit, name inferred) produces a single task."""
        tasks = self._get_tasks(api_client)
        assert "notify" in tasks

    def test_dependency_chain(self, api_client: AirflowAPI):
        """ingest_v1 + ingest_v2 -> check -> notify."""
        tasks = self._get_tasks(api_client)
        for t in [t for t in tasks if t.startswith(("ingest_v1.", "ingest_v2."))]:
            assert "check.not_null" in tasks[t]["downstream_task_ids"], (
                f"{t} should flow into check"
            )
        for t in [t for t in tasks if t.startswith("check.")]:
            assert "notify" in tasks[t]["downstream_task_ids"], f"{t} should flow into notify"

    def test_dag_has_default_team_tag(self, api_client: AirflowAPI):
        """explicit_naming uses default team=platform, tier=standard from ProjectDagArgs."""
        resp = api_client.get("/dags/explicit_naming")
        assert resp.status_code == 200
        tags = api_client.get_tags(resp.json())
        assert tags == {"team:platform", "standard"}


class TestDagArgsRendering:
    """Verify the full team/tier abstraction on the dag_args_test DAG."""

    DAG_ID = "dag_args_test"

    def _get_dag(self, api_client: AirflowAPI) -> dict:
        resp = api_client.get(f"/dags/{self.DAG_ID}")
        assert resp.status_code == 200
        return resp.json()

    def _get_tasks(self, api_client: AirflowAPI) -> list[dict]:
        resp = api_client.get(f"/dags/{self.DAG_ID}/tasks")
        assert resp.status_code == 200
        return resp.json()["tasks"]

    def test_description_applied(self, api_client: AirflowAPI):
        dag = self._get_dag(api_client)
        assert dag["description"] == (
            "Proves BlueprintDagArgs team/tier abstraction works end-to-end"
        )

    def test_team_tag_generated(self, api_client: AirflowAPI):
        dag = self._get_dag(api_client)
        tags = api_client.get_tags(dag)
        assert "team:analytics" in tags

    def test_tier_tag_generated(self, api_client: AirflowAPI):
        dag = self._get_dag(api_client)
        tags = api_client.get_tags(dag)
        assert "critical" in tags

    def test_only_derived_tags_present(self, api_client: AirflowAPI):
        dag = self._get_dag(api_client)
        tags = api_client.get_tags(dag)
        assert tags == {"team:analytics", "critical"}

    def test_owner_derived_from_team(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert len(tasks) > 0
        for task in tasks:
            assert task.get("owner") == "analytics", (
                f"Task {task['task_id']} owner={task.get('owner')!r}, expected 'analytics'"
            )

    def test_retries_derived_from_tier(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert len(tasks) > 0
        for task in tasks:
            assert task.get("retries") == 3, (
                f"Task {task['task_id']} retries={task.get('retries')}, expected 3"
            )

    def test_retry_delay_derived_from_tier(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        assert len(tasks) > 0
        for task in tasks:
            retry_delay = task.get("retry_delay")
            assert retry_delay is not None, f"Task {task['task_id']} has no retry_delay"

    def test_has_expected_tasks(self, api_client: AirflowAPI):
        tasks = self._get_tasks(api_client)
        task_ids = {t["task_id"] for t in tasks}
        assert task_ids == {"extract.validate", "extract.extract", "load"}

    def test_dependencies_correct(self, api_client: AirflowAPI):
        tasks = {t["task_id"]: t for t in self._get_tasks(api_client)}
        assert "load" in tasks["extract.validate"]["downstream_task_ids"] or (
            "load" in tasks["extract.extract"]["downstream_task_ids"]
        )


class TestDagArgsDefaults:
    """Verify that DagArgs defaults apply across DAGs that don't override them."""

    def test_simple_pipeline_default_tier(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline/tasks")
        assert resp.status_code == 200
        for task in resp.json()["tasks"]:
            assert task.get("retries") == 1

    def test_versioned_etl_critical_tier(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/versioned_etl/tasks")
        assert resp.status_code == 200
        for task in resp.json()["tasks"]:
            assert task.get("retries") == 3

    def test_default_team_when_unset(self, api_client: AirflowAPI):
        expected = {
            "simple_pipeline": "data-eng",
            "versioned_etl": "data-eng",
            "dag_args_test": "analytics",
        }
        for dag_id, expected_owner in expected.items():
            resp = api_client.get(f"/dags/{dag_id}/tasks")
            assert resp.status_code == 200
            for task in resp.json()["tasks"]:
                assert task.get("owner") == expected_owner, (
                    f"{dag_id}/{task['task_id']} owner={task.get('owner')!r}, "
                    f"expected {expected_owner!r}"
                )
