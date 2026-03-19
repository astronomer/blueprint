"""DAG loader -- place in your Airflow dags/ directory.

Discovers all *.dag.yaml files and builds them into Airflow DAGs.
Blueprint classes are auto-discovered from Python files in the same directory.
"""

from blueprint import build_all

build_all()
