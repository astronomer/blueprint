"""Airflow UI plugin: a YAML tab on the DAG page showing the source a DAG was built from.

Registered through the ``airflow.plugins`` entry point. The tab needs
``external_views``, which Airflow added in 3.1; older versions ignore the
attribute. Airflow 2 has no FastAPI, so the app is skipped there.
"""

import html
from typing import TYPE_CHECKING, Any

from airflow.plugins_manager import AirflowPlugin

from blueprint.builder import SOURCE_YAML_KEY

if TYPE_CHECKING:
    from fastapi import FastAPI

URL_PREFIX = "/blueprint"

NOT_FOUND_MESSAGE = "This DAG was not built from a Blueprint YAML file."

PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  body {{ margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  pre {{ margin: 0; padding: 16px; font-size: 13px; line-height: 1.5; overflow: auto; }}
</style>
</head>
<body><pre>{body}</pre></body>
</html>
"""


def source_yaml_from_default_args(default_args: Any) -> str | None:
    """Pull the embedded YAML out of serialized ``default_args``.

    Serialized dicts are wrapped as ``{"__type": "dict", "__var": {...}}``; a plain
    dict is accepted too.

    Args:
        default_args: The ``default_args`` entry of a serialized DAG.

    Returns:
        The YAML text, or None when the DAG has none.
    """
    if isinstance(default_args, dict) and "__var" in default_args:
        default_args = default_args["__var"]
    if not isinstance(default_args, dict):
        return None
    source = default_args.get(SOURCE_YAML_KEY)
    return source if isinstance(source, str) else None


def source_yaml(dag_id: str) -> str | None:
    """Read the YAML ``build_all_airflow_dags`` embedded in the latest serialized DAG.

    Args:
        dag_id: DAG id shown in the Airflow UI.

    Returns:
        The YAML text, or None when the DAG is unknown or has none.
    """
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        serialized = SerializedDagModel.get(dag_id, session=session)
        if serialized is None or serialized.data is None:
            return None
        return source_yaml_from_default_args(serialized.data["dag"].get("default_args"))


def create_app() -> "FastAPI":
    """Build the FastAPI app that serves DAG YAML pages.

    Returns:
        The configured FastAPI app.
    """
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse

    app = FastAPI(title="Blueprint")

    @app.get("/dags/{dag_id}/yaml", response_class=HTMLResponse)
    def dag_yaml(dag_id: str) -> str:
        text = source_yaml(dag_id) or NOT_FOUND_MESSAGE
        return PAGE.format(title=html.escape(dag_id), body=html.escape(text))

    return app


try:
    _fastapi_apps = [{"app": create_app(), "url_prefix": URL_PREFIX, "name": "Blueprint"}]
except ImportError:
    _fastapi_apps = []

_external_views = [
    {
        "name": "YAML",
        "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml",
        "destination": "dag",
        "url_route": "blueprint",
    }
]


class BlueprintPlugin(AirflowPlugin):
    """Show the YAML behind each Blueprint DAG in the Airflow UI."""

    name = "blueprint"
    fastapi_apps = _fastapi_apps
    external_views = _external_views
