"""Tests for the Airflow UI plugin."""

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from blueprint import plugin
from blueprint.plugin import (
    NOT_FOUND_MESSAGE,
    URL_PREFIX,
    BlueprintPlugin,
    create_app,
    source_yaml_from_default_args,
)

PIPELINE_YAML = "dag_id: plugin_pipeline\nteam: data-eng\nsteps:\n  s1:\n    blueprint: stub\n"


@pytest.fixture(autouse=True)
def sources(monkeypatch):
    by_dag: dict[str, str] = {}
    monkeypatch.setattr(plugin, "source_yaml", by_dag.get)
    return by_dag


def test_reads_yaml_from_serialized_default_args():
    serialized = {"__type": "dict", "__var": {"owner": "x", "blueprint_source": PIPELINE_YAML}}
    assert source_yaml_from_default_args(serialized) == PIPELINE_YAML


def test_reads_yaml_from_plain_default_args():
    assert source_yaml_from_default_args({"blueprint_source": PIPELINE_YAML}) == PIPELINE_YAML


@pytest.mark.parametrize("default_args", [None, {}, {"owner": "x"}, {"blueprint_source": 3}, "no"])
def test_no_yaml_when_default_args_lack_it(default_args):
    assert source_yaml_from_default_args(default_args) is None


def test_yaml_page_shows_source(sources):
    sources["plugin_pipeline"] = PIPELINE_YAML
    resp = TestClient(create_app()).get("/dags/plugin_pipeline/yaml")
    assert resp.status_code == 200
    assert "<title>plugin_pipeline</title>" in resp.text
    assert "<pre>dag_id: plugin_pipeline\nteam: data-eng" in resp.text


def test_yaml_page_explains_a_dag_without_yaml():
    resp = TestClient(create_app()).get("/dags/python_dag/yaml")
    assert resp.status_code == 200
    assert NOT_FOUND_MESSAGE in resp.text


def test_yaml_page_escapes_html(sources):
    sources["x"] = "dag_id: x\ndescription: <b>bold</b>\n"
    resp = TestClient(create_app()).get("/dags/x/yaml")
    assert "&lt;b&gt;bold&lt;/b&gt;" in resp.text
    assert "<b>bold</b>" not in resp.text


def test_plugin_tab_points_at_the_yaml_route():
    (view,) = BlueprintPlugin.external_views
    (fastapi_app,) = BlueprintPlugin.fastapi_apps
    assert view["name"] == "YAML"
    assert view["destination"] == "dag"
    assert view["href"] == f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml"
    assert fastapi_app["url_prefix"] == URL_PREFIX
    resp = TestClient(fastapi_app["app"]).get(
        view["href"].replace("{DAG_ID}", "nope")[len(URL_PREFIX) :]
    )
    assert resp.status_code == 200
    assert NOT_FOUND_MESSAGE in resp.text
