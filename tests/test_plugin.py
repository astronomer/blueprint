"""Tests for the Airflow UI plugin."""

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from blueprint.plugin.app import create_app, find_dag_yaml

PIPELINE_YAML = """\
dag_id: plugin_pipeline
steps:
  pull:
    blueprint: extract
    source_table: raw.events
"""


@pytest.fixture
def dags_folder(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "pipeline.dag.yaml").write_text(PIPELINE_YAML)
    (tmp_path / "broken.dag.yaml").write_text("dag_id: [unclosed")
    (tmp_path / "loader.py").write_text("from blueprint import build_all_airflow_dags\n")
    return tmp_path


def test_find_dag_yaml_matches_on_dag_id(dags_folder):
    expected = dags_folder / "nested" / "pipeline.dag.yaml"
    assert find_dag_yaml("plugin_pipeline", dags_folder) == expected


def test_find_dag_yaml_skips_unparseable_files(dags_folder):
    assert find_dag_yaml("missing", dags_folder) is None


def test_yaml_page_shows_source(dags_folder):
    client = TestClient(create_app(dags_folder))
    resp = client.get("/dags/plugin_pipeline/yaml")
    assert resp.status_code == 200
    assert "<pre>dag_id: plugin_pipeline" in resp.text
    assert "source_table: raw.events" in resp.text


def test_yaml_page_404_for_unknown_dag(dags_folder):
    client = TestClient(create_app(dags_folder))
    assert client.get("/dags/nope/yaml").status_code == 404


def test_yaml_page_escapes_html(tmp_path):
    (tmp_path / "x.dag.yaml").write_text("dag_id: x\ndescription: <b>bold</b>\nsteps: {}\n")
    client = TestClient(create_app(tmp_path))
    resp = client.get("/dags/x/yaml")
    assert "&lt;b&gt;bold&lt;/b&gt;" in resp.text
    assert "<b>bold</b>" not in resp.text


def test_plugin_registers_one_dag_tab():
    from blueprint.plugin import BlueprintPlugin, _airflow_version

    if _airflow_version() < (3, 1):
        assert BlueprintPlugin.external_views == []
        return
    assert [v["destination"] for v in BlueprintPlugin.external_views] == ["dag"]
    assert BlueprintPlugin.fastapi_apps[0]["url_prefix"] == "/blueprint"
