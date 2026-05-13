from pathlib import Path

from airflow import DAG

from blueprint import build_all_dags


def post_process(dag: DAG, yaml_path: Path) -> None:
    """Post-process every built DAG by appending a tag."""
    dag.tags = [*(dag.tags or []), "callback-verified"]


build_all_dags(on_dag_built=post_process)
