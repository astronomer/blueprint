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

from blueprint.builder import SOURCE_TAG_PREFIX
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


def source_from_tag(dag_id: str) -> str | None:
    """Read the ``blueprint:<path>`` tag ``build_all_airflow_dags`` put on the DAG.

    Args:
        dag_id: DAG id shown in the Airflow UI.

    Returns:
        The tagged path, relative to the dags folder, or None if the DAG has no tag.
    """
    from airflow.models.dag import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        dag_model = session.get(DagModel, dag_id)
        if dag_model is None:
            return None
        for tag in dag_model.tags:
            if tag.name.startswith(SOURCE_TAG_PREFIX):
                return tag.name[len(SOURCE_TAG_PREFIX) :]
    return None


def scan_for_dag_yaml(dag_id: str, dags_folder: Path) -> Path | None:
    """Walk the dags folder for the YAML whose raw ``dag_id`` field matches.

    Honors ``.airflowignore`` like the DAG processor. Reads the unrendered file,
    so a ``dag_id`` set through Jinja or a ``${var}`` reference is not found.

    Args:
        dag_id: DAG id shown in the Airflow UI.
        dags_folder: Root of the dags folder.

    Returns:
        The matching path, or None.
    """
    for path in discover_yaml_files(dags_folder, "*.dag.yaml"):
        try:
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (yaml.YAMLError, OSError):
            continue
        if isinstance(config, dict) and config.get("dag_id") == dag_id:
            return path
    return None


def find_dag_yaml(dag_id: str, dags_folder: Path, tagged: str | None) -> Path | None:
    """Find the YAML a DAG was built from: by its source tag, else by scanning.

    Args:
        dag_id: DAG id shown in the Airflow UI.
        dags_folder: Root of the dags folder. Tagged paths must stay inside it.
        tagged: The path from the DAG's source tag, if it has one.

    Returns:
        The matching path, or None.
    """
    if tagged:
        path = dags_folder / tagged
        if path.is_file() and path.resolve().is_relative_to(dags_folder.resolve()):
            return path
    return scan_for_dag_yaml(dag_id, dags_folder)


def not_found_message(tagged: str | None) -> str:
    """Explain why no YAML is shown, as well as the source tag allows.

    Args:
        tagged: The path from the DAG's source tag, if it has one.

    Returns:
        A sentence for the tab body.
    """
    if tagged:
        return f"This DAG was built from {tagged}, but that file is not on this server."
    return (
        "This DAG was not built from a Blueprint YAML file. "
        "It may be a Python DAG, or built with source_tags off."
    )


def create_app(dags_folder: Path | None = None) -> "FastAPI":
    """Build the FastAPI app that serves DAG YAML pages.

    Args:
        dags_folder: Folder to search. Defaults to Airflow's ``[core] dags_folder``.

    Returns:
        The configured FastAPI app.
    """
    from airflow.configuration import conf
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse

    folder = dags_folder or Path(conf.get("core", "dags_folder"))
    app = FastAPI(title="Blueprint")

    @app.get("/dags/{dag_id}/yaml", response_class=HTMLResponse)
    def dag_yaml(dag_id: str) -> str:
        tagged = source_from_tag(dag_id)
        path = find_dag_yaml(dag_id, folder, tagged)
        if path is None:
            message = not_found_message(tagged)
            return PAGE.format(title=html.escape(dag_id), body=html.escape(message))
        text = path.read_text(encoding="utf-8")
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
