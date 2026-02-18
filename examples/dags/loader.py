"""DAG loader -- place in your Airflow dags/ directory.

Discovers all *.dag.yaml files and builds them into Airflow DAGs.
Blueprint classes are auto-discovered from Python files in the same directory.
"""

from blueprint import build_all

build_all(
    dag_defaults={
        "default_args": {
            "owner": "data-team",
            "retries": 2,
            "retry_delay_seconds": 300,
        },
    }
)
