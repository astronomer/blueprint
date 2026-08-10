"""A consumer repository's loader.

There is no blueprints.py beside this file -- every template comes from the
installed acme-blueprints package, discovered through its entry point.
"""

from blueprint import build_all_airflow_dags

build_all_airflow_dags()
