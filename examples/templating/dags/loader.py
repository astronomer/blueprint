"""Loader supplying extra variables to the YAML templates.

Anything in template_context is available as a plain Jinja2 variable in every
*.dag.yaml this loader builds.
"""

import os

from blueprint import build_all_airflow_dags

build_all_airflow_dags(
    template_context={
        "region": os.environ.get("DEPLOY_REGION", "us-east-1"),
        "warehouse": "analytics",
    },
)
