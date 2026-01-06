"""Automatically load Blueprint DAGs from YAML configurations.

This file should be placed in your Airflow DAGs directory to automatically
discover and load all .dag.yaml files from the configs/ subdirectory.
"""

# Import the auto-load function from the blueprint package
from blueprint import auto_load_yaml_dags

# Automatically discover and register DAGs from YAML files
auto_load_yaml_dags()
