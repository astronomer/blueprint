"""Loader that selects the active profile.

The only thing Python decides is *which* profile is active. Every value the
profile chooses between stays in YAML, so `blueprint lint` resolves exactly
what the DAG processor does.
"""

import os

from blueprint import build_all_airflow_dags

profile = "prod" if os.environ.get("DEPLOY_ENV") == "prod" else "dev"

build_all_airflow_dags(profile=profile)
