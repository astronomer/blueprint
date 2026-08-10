"""Build one DAG per tenant with the Builder API instead of YAML.

Airflow's safe-mode scanner only parses a file whose contents mention both
"airflow" and "dag". This module says "airflow" nowhere except in this
docstring, which is reason enough to keep it.
"""

import json
from pathlib import Path

from blueprint import Builder, DAGConfig

TENANTS = json.loads((Path(__file__).parent / "tenants.json").read_text())

builder = Builder()

for tenant in TENANTS:
    # DAGConfig takes exactly the fields you would write in YAML: dag_id,
    # steps, and whatever your BlueprintDagArgs accepts.
    config = DAGConfig(
        dag_id=f"tenant_{tenant['id']}_etl",
        schedule=tenant["schedule"],
        description=f"ETL for {tenant['id']}",
        steps={
            "extract": {
                "blueprint": "extract",
                "source": tenant["source"],
                "datasets": tenant["datasets"],
            },
            "load": {
                "blueprint": "load",
                "depends_on": ["extract"],
                "target_schema": f"warehouse_{tenant['id']}",
            },
        },
    )

    dag = builder.build(config)

    # Airflow discovers DAGs by scanning module globals, so each one needs a
    # distinct name bound at module level.
    globals()[dag.dag_id] = dag
