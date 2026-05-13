from pathlib import Path

from airflow.models import DAG

from blueprint import build_all_dags


def add_mission_tags(dag: DAG, config_path: Path) -> None:
    """Add the source YAML filename as a DAG tag."""
    dag.tags = [*(dag.tags or []), f"source:{config_path.stem}"]


build_all_dags(
    on_dag_built=add_mission_tags,
    template_context={"agency": "Deep Space Network"},
)
