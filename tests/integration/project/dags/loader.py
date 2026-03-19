"""DAG loader for integration tests.

Discovers all *.dag.yaml files and builds them into Airflow DAGs.
Blueprint classes and the BlueprintDagArgs template are auto-discovered
from Python files in the same directory.
"""

from blueprint import build_all

build_all()
