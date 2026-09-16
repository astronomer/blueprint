"""Airflow UI plugin that adds a Blueprint tab showing a DAG's source YAML.

Registered through the ``airflow.plugins`` entry point. Requires Airflow 3.1+;
on older versions the plugin loads but registers nothing.
"""

from typing import Any

from airflow.plugins_manager import AirflowPlugin

URL_PREFIX = "/blueprint"


def _airflow_version() -> tuple[int, int]:
    try:
        from airflow import __version__ as airflow_version
        from packaging.version import Version
    except ImportError:
        return (0, 0)
    parsed = Version(airflow_version)
    return (parsed.major, parsed.minor)


def _surfaces() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if _airflow_version() < (3, 1):
        return [], []

    from blueprint.plugin.app import create_app

    fastapi_apps = [{"app": create_app(), "url_prefix": URL_PREFIX, "name": "Blueprint"}]
    external_views = [
        {
            "name": "Blueprint",
            "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml",
            "destination": "dag",
            "url_route": "blueprint",
        }
    ]
    return fastapi_apps, external_views


_fastapi_apps, _external_views = _surfaces()


class BlueprintPlugin(AirflowPlugin):
    """Expose the YAML behind each Blueprint DAG in the Airflow UI."""

    name = "blueprint"
    fastapi_apps = _fastapi_apps
    external_views = _external_views
