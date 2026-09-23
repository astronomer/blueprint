"""The Blueprint UI plugin against a running Airflow: tab, embedded YAML, and page."""

import httpx
import pytest
from packaging.version import Version

pytestmark = pytest.mark.integration

SIMPLE_YAML = (
    "dag_id: simple_pipeline\nteam: data-eng\nsteps:\n  process:\n    blueprint: transform\n"
)


def test_plugin_registers_a_yaml_tab(api_client):
    if api_client.airflow_version < Version("3.1"):
        pytest.skip("external_views arrived in Airflow 3.1")
    resp = api_client.get("/plugins")
    assert resp.status_code == 200, resp.text
    (plugin,) = [p for p in resp.json()["plugins"] if p["name"] == "blueprint"]
    (view,) = plugin["external_views"]
    assert view["name"] == "YAML"
    assert view["destination"] == "dag"
    assert view["href"] == "/blueprint/dags/{DAG_ID}/yaml"


def test_dag_carries_no_extra_tags(api_client):
    resp = api_client.get("/dags/simple_pipeline")
    assert resp.status_code == 200, resp.text
    assert not {t for t in api_client.get_tags(resp.json()) if t.startswith("blueprint")}


def test_yaml_page_serves_the_embedded_source(api_client):
    if api_client.airflow_version.major < 3:
        pytest.skip("the YAML page is a FastAPI app, which Airflow 2 lacks")
    resp = httpx.get(f"{api_client.base_url}/blueprint/dags/simple_pipeline/yaml", timeout=30)
    assert resp.status_code == 200, resp.text
    assert "<title>simple_pipeline</title>" in resp.text
    assert SIMPLE_YAML in resp.text


def test_yaml_page_explains_a_dag_without_yaml(api_client):
    if api_client.airflow_version.major < 3:
        pytest.skip("the YAML page is a FastAPI app, which Airflow 2 lacks")
    resp = httpx.get(f"{api_client.base_url}/blueprint/dags/nope/yaml", timeout=30)
    assert resp.status_code == 200
    assert "not built from a Blueprint YAML file" in resp.text
