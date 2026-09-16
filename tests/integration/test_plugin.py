"""The Blueprint UI plugin against a running Airflow: tag, tab, and YAML page."""

import httpx
import pytest

pytestmark = pytest.mark.integration


def test_plugin_registers_a_dag_tab(api_client):
    resp = api_client.get("/plugins")
    assert resp.status_code == 200, resp.text
    (plugin,) = [p for p in resp.json()["plugins"] if p["name"] == "blueprint"]
    (view,) = plugin["external_views"]
    assert view["destination"] == "dag"
    assert view["href"] == "/blueprint/dags/{DAG_ID}/yaml"


def test_dag_carries_its_source_tag(api_client):
    resp = api_client.get("/dags/simple_pipeline")
    assert resp.status_code == 200, resp.text
    assert "blueprint:simple.dag.yaml" in api_client.get_tags(resp.json())


def test_yaml_page_serves_the_source_file(api_client):
    resp = httpx.get(f"{api_client.base_url}/blueprint/dags/simple_pipeline/yaml", timeout=30)
    assert resp.status_code == 200, resp.text
    assert "<title>simple.dag.yaml</title>" in resp.text
    assert "dag_id: simple_pipeline" in resp.text


def test_yaml_page_explains_a_dag_without_yaml(api_client):
    resp = httpx.get(f"{api_client.base_url}/blueprint/dags/nope/yaml", timeout=30)
    assert resp.status_code == 200
    assert "not built from a Blueprint YAML file" in resp.text
