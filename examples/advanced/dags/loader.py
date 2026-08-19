import os
from pathlib import Path

from airflow.models import DAG

from blueprint import build_all_airflow_dags


def add_mission_tags(dag: DAG, config_path: Path) -> None:
    """Add the source YAML filename as a DAG tag."""
    dag.tags = [*(dag.tags or []), f"source:{config_path.stem}"]


# The only decision made in Python is *which* profile is active; every value
# lives in blueprint.vars.yaml or a DAG's own vars: block, so `blueprint lint`
# resolves exactly what the DAG processor does.
profile = "flight" if os.environ.get("MISSION_MODE") == "flight" else "sim"

build_all_airflow_dags(on_dag_built=add_mission_tags, profile=profile)
