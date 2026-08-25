"""One loader for the whole project.

Nothing here selects a template. Each DAG resolves its own from where the file
sits, so adding a directory with its own standards needs no loader change.
"""

from blueprint import build_all_airflow_dags

build_all_airflow_dags()
