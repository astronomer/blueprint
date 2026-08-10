"""Integrity tests: every DAG YAML in the repo still builds.

This is the test that catches a blueprint change breaking a YAML file nobody
touched. Run it in CI alongside `blueprint lint`.
"""

from pathlib import Path

import pytest
from blueprint import build_all_airflow_dags, validate_yaml

DAGS_DIR = Path(__file__).parent.parent / "dags"
YAML_FILES = sorted(DAGS_DIR.rglob("*.dag.yaml"))


def test_there_are_dag_files():
    # Guards against the suite silently passing because discovery broke.
    assert YAML_FILES, f"No *.dag.yaml found under {DAGS_DIR}"


@pytest.mark.parametrize("path", YAML_FILES, ids=lambda p: p.name)
def test_yaml_is_valid(path):
    """Same check as `blueprint lint`, as a test."""
    result = validate_yaml(str(path), template_dir=str(DAGS_DIR))
    assert result["dag_id"]


@pytest.fixture(scope="module")
def dags():
    built = build_all_airflow_dags(search_path=str(DAGS_DIR), register_globals={})
    return {dag.dag_id: dag for dag in built}


def test_all_dags_build(dags):
    assert set(dags) == {"customer_etl"}


def test_no_orphaned_tasks(dags):
    """Every task is connected, except the intended entry points."""
    dag = dags["customer_etl"]
    for task in dag.tasks:
        connected = task.upstream_task_ids or task.downstream_task_ids
        assert connected, f"{task.task_id} is not wired to anything"


def test_expected_structure(dags):
    dag = dags["customer_etl"]
    assert {task.task_id for task in dag.tasks} == {
        "extract_crm.validate",
        "extract_crm.pull_customers",
        "extract_crm.pull_contacts",
        "load_customers",
    }


def test_load_waits_for_extract(dags):
    dag = dags["customer_etl"]
    load = dag.get_task("load_customers")
    assert "extract_crm.pull_customers" in load.upstream_task_ids
