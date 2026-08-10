"""Shared fixtures.

Blueprints build Airflow objects, and a TaskGroup can only be created inside a
DAG context -- so most render() tests need one.
"""

from datetime import datetime

import pytest

try:  # Airflow 3
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG

DAGS_DIR = "dags"


@pytest.fixture
def dag():
    """A throwaway DAG to render blueprints into."""
    with DAG(
        dag_id="test_dag",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
    ) as test_dag:
        yield test_dag


def render(blueprint, config, step_id="step"):
    """Render a blueprint the way the builder would."""
    instance = blueprint()
    instance.step_id = step_id
    return instance.render(config)


def task_ids(group) -> set[str]:
    """Leaf task IDs inside a rendered TaskGroup, relative to the group."""
    prefix = f"{group.group_id}."
    return {
        task.task_id.removeprefix(prefix) for task in group
    }
