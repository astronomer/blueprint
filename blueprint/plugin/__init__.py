"""Airflow UI plugin: the YAML and blueprint code behind each DAG.

Registered automatically via the ``airflow.plugins`` entry point when
airflow-blueprint is installed. On Airflow 3 it mounts a FastAPI app at
``/blueprint`` under the API server; on Airflow 3.1+ it adds a
"Blueprint" tab at every level of the UI:

- DAG and DAG-run pages show the source YAML the DAG was built from,
  with the blueprint Python underneath
- task and task-instance pages show that step's config and blueprint
  source, resolved to what actually ran where possible

On Airflow 2.x the plugin loads but registers nothing.
"""

import logging
from typing import Any

from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger(__name__)

URL_PREFIX = "/blueprint"


def _airflow_version() -> tuple[int, int]:
    """Return the running Airflow (major, minor), or (0, 0) if unknown."""
    try:
        from airflow import __version__ as airflow_version
        from packaging.version import Version

        v = Version(airflow_version)
    except Exception:
        return (0, 0)
    return (v.major, v.minor)


def _build_surfaces() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build the fastapi_apps and external_views lists for this Airflow version."""
    version = _airflow_version()
    if version < (3, 0):
        return [], []

    try:
        from blueprint.plugin.app import create_app

        app = create_app()
    except ImportError as e:
        logger.warning("Blueprint UI plugin disabled: %s", e)
        return [], []

    fastapi_apps = [{"app": app, "url_prefix": URL_PREFIX, "name": "Blueprint"}]

    if version < (3, 1):
        return fastapi_apps, []

    external_views = [
        {
            "name": "Blueprint",
            "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml",
            "destination": "dag",
            "url_route": "blueprint_dag",
        },
        {
            "name": "Blueprint",
            "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml",
            "destination": "dag_run",
            "url_route": "blueprint_dag_run",
        },
        {
            "name": "Blueprint",
            "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/tasks/{{TASK_ID}}",
            "destination": "task",
            "url_route": "blueprint_task",
        },
        {
            "name": "Blueprint",
            "href": (
                f"{URL_PREFIX}/dags/{{DAG_ID}}/tasks/{{TASK_ID}}"
                "?run_id={RUN_ID}&map_index={MAP_INDEX}"
            ),
            "destination": "task_instance",
            "url_route": "blueprint_task_instance",
        },
    ]
    return fastapi_apps, external_views


_fastapi_apps, _external_views = _build_surfaces()


class BlueprintPlugin(AirflowPlugin):
    """Airflow plugin exposing Blueprint's YAML source and catalog views."""

    name = "blueprint"

    fastapi_apps = _fastapi_apps
    external_views = _external_views
