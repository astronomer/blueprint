"""Tests for the Airflow UI plugin: data helpers, FastAPI views, registration."""

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from blueprint.plugin.app import (
    clear_cache,
    create_app,
    resolve_dag_source,
    resolve_task_step,
)

BLUEPRINTS_PY = '''
from pydantic import BaseModel
from blueprint.core import Blueprint

class ExtractConfig(BaseModel):
    source_table: str
    batch_size: int = 1000

class Extract(Blueprint[ExtractConfig]):
    """Extract data from a source."""

    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo extract")

class ExtractV2Config(BaseModel):
    sources: list[str]

class ExtractV2(Blueprint[ExtractV2Config]):
    """Extract v2 with multi-source."""

    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo extract2")

class LoadConfig(BaseModel):
    target_table: str

class Load(Blueprint[LoadConfig]):
    """Load data to a target."""

    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo load")
'''

PIPELINE_YAML = """\
dag_id: plugin_pipeline
steps:
  pull:
    blueprint: extract
    version: 1
    source_table: raw.events
  push:
    blueprint: load
    depends_on: [pull]
    target_table: warehouse.events
"""

JINJA_YAML = """\
dag_id: plugin_jinja
description: "{{ team_name }}"
steps:
  pull:
    blueprint: extract
    version: 2
    sources: ["a"]
"""


@pytest.fixture
def dags_folder(tmp_path):
    clear_cache()
    (tmp_path / "blueprints.py").write_text(BLUEPRINTS_PY)
    (tmp_path / "pipeline.dag.yaml").write_text(PIPELINE_YAML)
    (tmp_path / "templated.dag.yaml").write_text(JINJA_YAML)
    drafts = tmp_path / "drafts"
    drafts.mkdir()
    (drafts / "wip.dag.yaml").write_text("dag_id: plugin_wip\nsteps:\n  s:\n    blueprint: load\n")
    (tmp_path / ".airflowignore").write_text("drafts\n")
    yield tmp_path
    clear_cache()


@pytest.fixture
def client(dags_folder):
    return TestClient(create_app(dags_folder=dags_folder))


class TestDagSourceResolution:
    def test_resolves_by_scanning(self, dags_folder):
        path = resolve_dag_source("plugin_pipeline", dags_folder)
        assert path == dags_folder / "pipeline.dag.yaml"

    def test_resolves_templated_yaml(self, dags_folder):
        path = resolve_dag_source("plugin_jinja", dags_folder)
        assert path == dags_folder / "templated.dag.yaml"

    def test_unknown_dag_returns_none(self, dags_folder):
        assert resolve_dag_source("nope", dags_folder) is None

    def test_airflowignore_respected(self, dags_folder):
        assert resolve_dag_source("plugin_wip", dags_folder) is None


class TestStampedValueCleaning:
    def test_truncated_value_dropped(self):
        from blueprint.plugin.app import _clean_stamped_value

        truncated = (
            "Truncated. You can change this behaviour in "
            '[core]max_templated_field_length. \'"""Blueprint definitions...\''
        )
        assert _clean_stamped_value(truncated) is None

    def test_normal_value_kept(self):
        from blueprint.plugin.app import _clean_stamped_value

        assert _clean_stamped_value("blueprint: extract\n") == "blueprint: extract\n"

    def test_non_string_dropped(self):
        from blueprint.plugin.app import _clean_stamped_value

        assert _clean_stamped_value(None) is None
        assert _clean_stamped_value(123) is None


class TestTaskStepResolution:
    def test_exact_task_id(self, dags_folder):
        match = resolve_task_step("plugin_pipeline", "pull", dags_folder)
        assert match is not None
        step_name, step = match
        assert step_name == "pull"
        assert step["blueprint"] == "extract"

    def test_task_group_child(self, dags_folder):
        match = resolve_task_step("plugin_pipeline", "pull.download", dags_folder)
        assert match is not None
        assert match[0] == "pull"

    def test_unknown_task(self, dags_folder):
        assert resolve_task_step("plugin_pipeline", "nope", dags_folder) is None

    def test_unknown_dag(self, dags_folder):
        assert resolve_task_step("nope", "pull", dags_folder) is None


class TestTaskStepPage:
    def test_pinned_step(self, client):
        resp = client.get("/dags/plugin_pipeline/tasks/pull")
        assert resp.status_code == 200
        assert "extract" in resp.text
        assert "v1 · pinned" in resp.text
        assert "raw.events" in resp.text
        assert "source YAML on disk" in resp.text
        assert "ExtractConfig" in resp.text
        assert "· current file" in resp.text

    def test_unpinned_step_resolves_latest(self, client):
        resp = client.get("/dags/plugin_pipeline/tasks/push")
        assert resp.status_code == 200
        assert "load" in resp.text
        assert "v1 · latest" in resp.text

    def test_unknown_task_404(self, client):
        resp = client.get("/dags/plugin_pipeline/tasks/nope")
        assert resp.status_code == 404
        assert "No blueprint step found" in resp.text

    def test_group_page_recovers_step_from_referer(self, client):
        resp = client.get(
            "/dags/plugin_pipeline/tasks/{TASK_ID}",
            headers={"referer": "http://localhost:8080/dags/plugin_pipeline/tasks/group/pull"},
        )
        assert resp.status_code == 200
        assert "extract" in resp.text
        assert "v1 · pinned" in resp.text

    def test_group_page_referer_with_run(self, client, monkeypatch):
        import blueprint.plugin.app as app_module

        seen = {}

        def fake_rtif(_dag_id, run_id, task_id, _map_index):
            seen.update(run_id=run_id, task_id=task_id)
            return None, None

        monkeypatch.setattr(app_module, "_step_context_from_rtif", fake_rtif)
        resp = client.get(
            "/dags/plugin_pipeline/tasks/{TASK_ID}",
            params={"run_id": "{RUN_ID}"},
            headers={
                "referer": (
                    "http://localhost:8080/dags/plugin_pipeline"
                    "/runs/manual__2026-07-24T00%3A00%3A00%2B00%3A00/tasks/group/pull"
                )
            },
        )
        assert resp.status_code == 200
        assert seen == {"run_id": "manual__2026-07-24T00:00:00+00:00", "task_id": "pull"}

    def test_unsubstituted_token_without_referer_404(self, client):
        resp = client.get("/dags/plugin_pipeline/tasks/{TASK_ID}")
        assert resp.status_code == 404

    def test_unsubstituted_tokens_ignored(self, client):
        resp = client.get(
            "/dags/plugin_pipeline/tasks/pull",
            params={"run_id": "{RUN_ID}", "map_index": "{MAP_INDEX}"},
        )
        assert resp.status_code == 200
        assert "source YAML on disk" in resp.text

    def test_serialized_context_preferred_over_yaml(self, client, monkeypatch):
        import blueprint.plugin.app as app_module

        stamped = "blueprint: extract\nversion: 2\nsource: pipeline.dag.yaml\nsources:\n- a\n"
        monkeypatch.setattr(
            app_module,
            "_step_context_from_serialized_dag",
            lambda *_args: (stamped, "# serialized copy\nclass ExtractV2: ..."),
        )
        resp = client.get("/dags/plugin_pipeline/tasks/pull")
        assert resp.status_code == 200
        assert "v2" in resp.text
        assert "current serialized DAG" in resp.text
        assert "# serialized copy" in resp.text
        assert "· as built" in resp.text

    def test_run_context_preferred_when_run_id_given(self, client, monkeypatch):
        import blueprint.plugin.app as app_module

        calls = {}

        def fake_rtif(_dag_id, run_id, _task_id, map_index):
            calls.update(run_id=run_id, map_index=map_index)
            return "blueprint: extract\nversion: 1\nsource_table: overridden.events\n", None

        monkeypatch.setattr(app_module, "_step_context_from_rtif", fake_rtif)
        resp = client.get(
            "/dags/plugin_pipeline/tasks/pull",
            params={"run_id": "manual__2026-07-24", "map_index": "-1"},
        )
        assert resp.status_code == 200
        assert calls == {"run_id": "manual__2026-07-24", "map_index": -1}
        assert "overridden.events" in resp.text
        assert "as rendered for run" in resp.text

    def test_run_id_plus_recovered_from_space(self, client, monkeypatch):
        import blueprint.plugin.app as app_module

        seen = []

        def fake_rtif(_dag_id, run_id, _task_id, _map_index):
            seen.append(run_id)
            return ("blueprint: extract\nversion: 1\n", None) if "+" in run_id else (None, None)

        monkeypatch.setattr(app_module, "_step_context_from_rtif", fake_rtif)
        resp = client.get(
            "/dags/plugin_pipeline/tasks/pull",
            params={"run_id": "scheduled__2026-07-24T00:00:00 00:00"},
        )
        assert resp.status_code == 200
        assert seen == [
            "scheduled__2026-07-24T00:00:00 00:00",
            "scheduled__2026-07-24T00:00:00+00:00",
        ]
        assert "as rendered for run" in resp.text


class TestDagYamlPage:
    def test_shows_yaml(self, client):
        resp = client.get("/dags/plugin_pipeline/yaml")
        assert resp.status_code == 200
        assert "plugin_pipeline" in resp.text
        assert "warehouse.events" in resp.text
        assert "pipeline.dag.yaml" in resp.text

    def test_shows_blueprint_code_sections(self, client):
        resp = client.get("/dags/plugin_pipeline/yaml")
        assert resp.status_code == 200
        assert "Blueprint code" in resp.text
        assert "ExtractConfig" in resp.text
        assert "LoadConfig" in resp.text

    def test_unknown_dag_404(self, client):
        resp = client.get("/dags/nope/yaml")
        assert resp.status_code == 404
        assert "No source YAML found" in resp.text


class TestPluginRegistration:
    def test_surfaces_on_airflow_31(self, monkeypatch):
        import blueprint.plugin as plugin_module

        monkeypatch.setattr(plugin_module, "_airflow_version", lambda: (3, 1))
        fastapi_apps, external_views = plugin_module._build_surfaces()
        assert fastapi_apps[0]["url_prefix"] == "/blueprint"
        destinations = {v["destination"] for v in external_views}
        assert destinations == {"dag", "dag_run", "task", "task_instance"}
        (dag_view,) = [v for v in external_views if v["destination"] == "dag"]
        assert "{DAG_ID}" in dag_view["href"]
        assert {v["name"] for v in external_views} == {"Blueprint"}
        (task_view,) = [v for v in external_views if v["destination"] == "task"]
        assert "{TASK_ID}" in task_view["href"]

    def test_no_external_views_on_airflow_30(self, monkeypatch):
        import blueprint.plugin as plugin_module

        monkeypatch.setattr(plugin_module, "_airflow_version", lambda: (3, 0))
        fastapi_apps, external_views = plugin_module._build_surfaces()
        assert fastapi_apps
        assert external_views == []

    def test_disabled_on_airflow_2(self, monkeypatch):
        import blueprint.plugin as plugin_module

        monkeypatch.setattr(plugin_module, "_airflow_version", lambda: (2, 10))
        assert plugin_module._build_surfaces() == ([], [])
