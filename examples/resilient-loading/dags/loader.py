"""Loader that tolerates a broken DAG file.

By default one unbuildable YAML file aborts the whole load, taking every other
DAG in the folder with it. skip_invalid_dags logs the failure and carries on.
"""

from blueprint import build_all_airflow_dags

build_all_airflow_dags(skip_invalid_dags=True)
