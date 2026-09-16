"""Airflow UI plugin: a Blueprint tab on the DAG page showing the source YAML.

Registered through the ``airflow.plugins`` entry point. The tab needs
``external_views``, which Airflow added in 3.1; older versions ignore the
attribute. Airflow 2 has no FastAPI, so the app is skipped there.
"""

import html
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from airflow.plugins_manager import AirflowPlugin

from blueprint.loaders import discover_yaml_files

if TYPE_CHECKING:
    from fastapi import FastAPI

URL_PREFIX = "/blueprint"

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


def find_dag_yaml(dag_id: str, dags_folder: Path) -> tuple[Path, str] | None:
    """Find the DAG YAML whose raw ``dag_id`` field matches.

    Walks the folder the way the DAG processor does, honoring ``.airflowignore``.
    Matches on the unrendered file, so a ``dag_id`` set through Jinja or a
    ``${var}`` reference is not found.

    Args:
        dag_id: DAG id shown in the Airflow UI.
        dags_folder: Root of the DAGs folder.

    Returns:
        The matching path and its text, or None.
    """
    for path in discover_yaml_files(dags_folder, "*.dag.yaml"):
        try:
            text = path.read_text(encoding="utf-8")
            config = yaml.safe_load(text)
        except (yaml.YAMLError, OSError):
            continue
        if isinstance(config, dict) and config.get("dag_id") == dag_id:
            return path, text
    return None


def create_app(dags_folder: Path | None = None) -> "FastAPI":
    """Build the FastAPI app that serves DAG YAML pages.

    Args:
        dags_folder: Folder to search. Defaults to Airflow's ``[core] dags_folder``.

    Returns:
        The configured FastAPI app.
    """
    from airflow.configuration import conf
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse

    folder = dags_folder or Path(conf.get("core", "dags_folder"))
    app = FastAPI(title="Blueprint")

    @app.get("/dags/{dag_id}/yaml", response_class=HTMLResponse)
    def dag_yaml(dag_id: str) -> str:
        found = find_dag_yaml(dag_id, folder)
        if found is None:
            raise HTTPException(status_code=404, detail=f"No Blueprint YAML found for {dag_id}")
        path, text = found
        return PAGE.format(title=html.escape(path.name), body=html.escape(text))

    return app


try:
    _fastapi_apps = [{"app": create_app(), "url_prefix": URL_PREFIX, "name": "Blueprint"}]
except ImportError:
    _fastapi_apps = []

_external_views = [
    {
        "name": "Blueprint",
        "href": f"{URL_PREFIX}/dags/{{DAG_ID}}/yaml",
        "destination": "dag",
        "url_route": "blueprint",
    }
]


class BlueprintPlugin(AirflowPlugin):
    """Expose the YAML behind each Blueprint DAG in the Airflow UI."""

    name = "blueprint"
    fastapi_apps = _fastapi_apps
    external_views = _external_views
