"""Shared test fixtures and utilities."""

from pathlib import Path

STUB_BLUEPRINT_SOURCE = """
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
"""

DAG_ARGS_SOURCE = """
from typing import Any
from pydantic import BaseModel, ConfigDict
from blueprint.core import BlueprintDagArgs

class {cls}Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    {field}: str = "unset"

class {cls}(BlueprintDagArgs[{cls}Config]{default}):
    def render(self, config: {cls}Config) -> dict[str, Any]:
        return {{"tags": ["{cls}"]}}
"""


def write_dag_args(
    directory: Path,
    cls: str,
    field: str = "x",
    default: bool = False,
    file_name: str = "dag_args.py",
) -> Path:
    """Write one BlueprintDagArgs template into a directory."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / file_name
    path.write_text(
        DAG_ARGS_SOURCE.format(cls=cls, field=field, default=", default=True" if default else "")
    )
    return path


def write_stub_blueprint(directory: Path, file_name: str = "blueprints.py") -> Path:
    """Write the minimal blueprint the DAG YAML fixtures reference."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / file_name
    path.write_text(STUB_BLUEPRINT_SOURCE)
    return path


def write_dag_yaml(directory: Path, dag_id: str, top_level: str = "") -> Path:
    """Write a one-step DAG YAML file, optionally with extra top-level fields."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dag_id}.dag.yaml"
    path.write_text(f"dag_id: {dag_id}\n{top_level}steps:\n  s1:\n    blueprint: stub\n")
    return path
