"""Tests for the Airflow UI plugin."""

import pytest

pytest.importorskip("fastapi")

from conftest import write_dag_yaml
from fastapi.testclient import TestClient

from blueprint import plugin
from blueprint.plugin import URL_PREFIX, BlueprintPlugin, create_app, find_dag_yaml


@pytest.fixture(autouse=True)
def tagged(monkeypatch):
    tags: dict[str, str] = {}
    monkeypatch.setattr(plugin, "source_from_tag", tags.get)
    return tags


@pytest.fixture
def dags_folder(tmp_path):
    write_dag_yaml(tmp_path / "nested", "plugin_pipeline", top_level="team: data-eng\n")
    write_dag_yaml(tmp_path / "drafts", "ignored_pipeline")
    (tmp_path / ".airflowignore").write_text("drafts/\n")
    (tmp_path / "broken.dag.yaml").write_text("dag_id: [unclosed")
    return tmp_path


def test_tag_wins_without_a_scan(dags_folder):
    (dags_folder / "nested" / "plugin_pipeline.dag.yaml").write_text("dag_id: renamed\n")
    found = find_dag_yaml("plugin_pipeline", dags_folder, "nested/plugin_pipeline.dag.yaml")
    assert found == dags_folder / "nested" / "plugin_pipeline.dag.yaml"


def test_tag_outside_the_dags_folder_falls_back_to_scan(dags_folder):
    found = find_dag_yaml("plugin_pipeline", dags_folder, "../../etc/passwd")
    assert found == dags_folder / "nested" / "plugin_pipeline.dag.yaml"


def test_stale_tag_falls_back_to_scan(dags_folder):
    found = find_dag_yaml("plugin_pipeline", dags_folder, "moved/plugin_pipeline.dag.yaml")
    assert found == dags_folder / "nested" / "plugin_pipeline.dag.yaml"


def test_scan_matches_on_dag_id_past_broken_files(dags_folder):
    found = find_dag_yaml("plugin_pipeline", dags_folder, None)
    assert found == dags_folder / "nested" / "plugin_pipeline.dag.yaml"


def test_scan_honors_airflowignore(dags_folder):
    assert find_dag_yaml("ignored_pipeline", dags_folder, None) is None


def test_yaml_page_shows_source(dags_folder):
    resp = TestClient(create_app(dags_folder)).get("/dags/plugin_pipeline/yaml")
    assert resp.status_code == 200
    assert "<title>plugin_pipeline.dag.yaml</title>" in resp.text
    assert "<pre>dag_id: plugin_pipeline\nteam: data-eng" in resp.text


def test_yaml_page_explains_a_dag_without_yaml(dags_folder):
    resp = TestClient(create_app(dags_folder)).get("/dags/python_dag/yaml")
    assert resp.status_code == 200
    assert "not built from a Blueprint YAML file" in resp.text
    assert "source_tags off" in resp.text


def test_yaml_page_names_a_tagged_file_missing_from_this_server(dags_folder, tagged):
    tagged["moved_dag"] = "elsewhere/moved.dag.yaml"
    resp = TestClient(create_app(dags_folder)).get("/dags/moved_dag/yaml")
    assert resp.status_code == 200
    assert "built from elsewhere/moved.dag.yaml, but that file is not on this server" in resp.text


def test_yaml_page_escapes_html(tmp_path):
    write_dag_yaml(tmp_path, "x", top_level="description: <b>bold</b>\n")
    resp = TestClient(create_app(tmp_path)).get("/dags/x/yaml")
    assert "&lt;b&gt;bold&lt;/b&gt;" in resp.text
    assert "<b>bold</b>" not in resp.text


def test_plugin_tab_points_at_the_yaml_route():
    (view,) = BlueprintPlugin.external_views
    (fastapi_app,) = BlueprintPlugin.fastapi_apps
    assert view["destination"] == "dag"
    assert view["href"] == f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml"
    assert fastapi_app["url_prefix"] == URL_PREFIX
    resp = TestClient(fastapi_app["app"]).get(
        view["href"].replace("{DAG_ID}", "nope")[len(URL_PREFIX) :]
    )
    assert resp.status_code == 200
    assert "not built from a Blueprint YAML file" in resp.text
