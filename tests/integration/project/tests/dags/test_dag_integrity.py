"""DAG integrity tests run via `astro dev pytest --standalone`.

Validates that all DAGs load without import errors and meet basic standards.
"""

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


EXPECTED_DAG_IDS = {"simple_pipeline", "versioned_etl", "dag_args_test"}


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_no_import_errors(rel_path, rv):
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_tags(dag_id, dag, fileloc):
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"


def test_expected_dags_present():
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
    loaded_ids = set(dag_bag.dags.keys())
    missing = EXPECTED_DAG_IDS - loaded_ids
    assert not missing, f"Missing DAGs: {missing}"


def test_dag_args_team_tag_generated():
    """Verify that the team/tier abstraction generates the expected auto-tags."""
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["dag_args_test"]
    tags = set(dag.tags)
    assert "team:analytics" in tags, f"Expected 'team:analytics' tag, got {tags}"
    assert "critical" in tags, f"Expected 'critical' tag, got {tags}"
