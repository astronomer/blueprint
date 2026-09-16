"""FastAPI app serving a DAG's source YAML as a plain HTML page."""

from __future__ import annotations

import html
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from fastapi import FastAPI

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


def default_dags_folder() -> Path:
    from airflow.configuration import conf

    return Path(conf.get("core", "dags_folder"))


def find_dag_yaml(dag_id: str, dags_folder: Path) -> Path | None:
    for path in sorted(dags_folder.rglob("*.dag.yaml")):
        try:
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (yaml.YAMLError, OSError):
            continue
        if isinstance(config, dict) and config.get("dag_id") == dag_id:
            return path
    return None


def render_page(title: str, body: str) -> str:
    return PAGE.format(title=html.escape(title), body=html.escape(body))


def create_app(dags_folder: Path | None = None) -> FastAPI:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse

    app = FastAPI(title="Blueprint")

    @app.get("/dags/{dag_id}/yaml", response_class=HTMLResponse)
    def dag_yaml(dag_id: str) -> str:
        folder = dags_folder or default_dags_folder()
        path = find_dag_yaml(dag_id, folder)
        if path is None:
            raise HTTPException(status_code=404, detail=f"No Blueprint YAML found for {dag_id}")
        return render_page(path.name, path.read_text(encoding="utf-8"))

    return app
