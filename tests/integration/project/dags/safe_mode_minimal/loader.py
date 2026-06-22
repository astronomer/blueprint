from blueprint import build_all_airflow_dags


def _tag(dag, yaml_path):
    dag.tags = [*(dag.tags or []), "safe-mode-probe"]


build_all_airflow_dags(pattern="*.safe.yaml", on_dag_built=_tag)
