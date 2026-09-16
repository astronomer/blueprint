"""Tests for the Airflow UI plugin."""

import pytest

pytest.importorskip("fastapi")

from conftest import write_dag_yaml
from fastapi.testclient import TestClient

from blueprint.plugin import URL_PREFIX, BlueprintPlugin, create_app, find_dag_yaml


@pytest.fixture
def dags_folder(tmp_path):
    write_dag_yaml(tmp_path / "nested", "plugin_pipeline", top_level="team: data-eng\n")
    write_dag_yaml(tmp_path / "drafts", "ignored_pipeline")
    (tmp_path / ".airflowignore").write_text("drafts/\n")
    (tmp_path / "broken.dag.yaml").write_text("dag_id: [unclosed")
    return tmp_path


def test_find_dag_yaml_matches_on_dag_id_past_broken_files(dags_folder):
    path, text = find_dag_yaml("plugin_pipeline", dags_folder)
    assert path == dags_folder / "nested" / "plugin_pipeline.dag.yaml"
    assert text.startswith("dag_id: plugin_pipeline\n")


def test_find_dag_yaml_honors_airflowignore(dags_folder):
    assert find_dag_yaml("ignored_pipeline", dags_folder) is None


def test_yaml_page_shows_source(dags_folder):
    resp = TestClient(create_app(dags_folder)).get("/dags/plugin_pipeline/yaml")
    assert resp.status_code == 200
    assert "<title>plugin_pipeline.dag.yaml</title>" in resp.text
    assert "<pre>dag_id: plugin_pipeline\nteam: data-eng" in resp.text


def test_yaml_page_404_for_unknown_dag(dags_folder):
    assert TestClient(create_app(dags_folder)).get("/dags/nope/yaml").status_code == 404


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
    assert resp.status_code == 404
