"""Integration tests for the Blueprint UI plugin against a live Airflow.

Verifies the plugin registers with the API server, the mounted FastAPI app
serves the YAML and blueprint-code pages, and DAGs carry the
``blueprint:<path>`` source tag.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from .conftest import AirflowAPI

pytestmark = pytest.mark.integration


class TestPluginRegistration:
    def test_plugin_listed_by_api(self, api_client: AirflowAPI):
        resp = api_client.get("/plugins")
        assert resp.status_code == 200, resp.text
        plugins = {p["name"]: p for p in resp.json()["plugins"]}
        assert "blueprint" in plugins

    def test_external_views_registered(self, api_client: AirflowAPI):
        resp = api_client.get("/plugins")
        assert resp.status_code == 200, resp.text
        (plugin,) = [p for p in resp.json()["plugins"] if p["name"] == "blueprint"]
        views = plugin.get("external_views") or []
        destinations = {v.get("destination") for v in views}
        assert destinations == {"dag", "dag_run", "task", "task_instance"}
        assert {v.get("name") for v in views} == {"Blueprint"}


class TestDagYamlPage:
    def test_shows_source_yaml(self, api_client: AirflowAPI):
        resp = api_client.client().get("/blueprint/dags/simple_pipeline/yaml")
        assert resp.status_code == 200, resp.text
        assert "simple_pipeline" in resp.text
        assert "simple.dag.yaml" in resp.text
        assert "Blueprint code" in resp.text
        assert "TransformConfig" in resp.text

    def test_unknown_dag_returns_404(self, api_client: AirflowAPI):
        resp = api_client.client().get("/blueprint/dags/no_such_dag/yaml")
        assert resp.status_code == 404
        assert "No source YAML found" in resp.text


class TestTaskStepPage:
    def test_task_group_child_resolves_step(self, api_client: AirflowAPI):
        resp = api_client.client().get("/blueprint/dags/simple_pipeline/tasks/process.clean")
        assert resp.status_code == 200, resp.text
        assert "transform" in resp.text
        assert "process" in resp.text

    def test_serialized_dag_provenance_without_run(self, api_client: AirflowAPI):
        resp = api_client.client().get("/blueprint/dags/simple_pipeline/tasks/process.clean")
        assert resp.status_code == 200, resp.text
        assert "current serialized DAG" in resp.text
        assert "TransformConfig" in resp.text
        # The blueprint file exceeds [core] max_templated_field_length, so the
        # serialized code copy is truncated and the code falls back to disk.
        assert "· current file" in resp.text

    def test_run_provenance_with_run_id(self, api_client: AirflowAPI):
        from .test_dag_execution import _trigger_dag, _unpause_dag, _wait_for_dag_run

        dag_id = "simple_pipeline"
        _unpause_dag(api_client, dag_id)
        run_id = _trigger_dag(api_client, dag_id)
        result = _wait_for_dag_run(api_client, dag_id, run_id)
        assert result["state"] == "success", result

        resp = api_client.client().get(
            f"/blueprint/dags/{dag_id}/tasks/process.clean",
            params={"run_id": run_id, "map_index": "-1"},
        )
        assert resp.status_code == 200, resp.text
        assert "as rendered for run" in resp.text
        assert run_id in resp.text

    def test_unknown_task_returns_404(self, api_client: AirflowAPI):
        resp = api_client.client().get("/blueprint/dags/simple_pipeline/tasks/nope")
        assert resp.status_code == 404
        assert "No blueprint step found" in resp.text


class TestSourceTags:
    def test_dag_carries_source_tag(self, api_client: AirflowAPI):
        resp = api_client.get("/dags/simple_pipeline")
        assert resp.status_code == 200, resp.text
        tags = api_client.get_tags(resp.json())
        assert "blueprint:simple.dag.yaml" in tags
