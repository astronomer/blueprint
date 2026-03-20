"""DAG loader for integration tests.

Discovers all *.dag.yaml files and builds them into Airflow DAGs.
Blueprint classes and the BlueprintDagArgs template are auto-discovered
from Python files in the same directory.

Uses on_dag_built to post-process every DAG: appends a "callback-verified"
tag, proving the callback materially changed the DAG that Airflow sees.
"""

from pathlib import Path

from airflow import DAG

from blueprint import build_all


def post_process(dag: DAG, yaml_path: Path) -> None:
    """Post-process every built DAG by appending a tag."""
    dag.tags = [*(dag.tags or []), "callback-verified"]


build_all(on_dag_built=post_process)
