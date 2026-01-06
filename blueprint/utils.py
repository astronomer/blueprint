"""Common utilities used across blueprint modules."""

import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Handle different Python versions for tomllib
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib  # type: ignore[import-not-found]
    except ImportError:
        tomllib = None


def load_config() -> Dict[str, Any]:
    """Load config from blueprint.toml if it exists."""
    for filename in ["blueprint.toml", ".blueprint.toml"]:
        config_path = Path(filename)
        if config_path.exists():
            if tomllib is None:
                msg = "tomli is required for Python <3.11. Install with: pip install tomli"
                raise ImportError(msg)
            with config_path.open("rb") as f:
                return tomllib.load(f)
    return {}


def get_template_path(cli_value: Optional[str] = None) -> str:
    """Get template path with precedence: CLI > env > config > default."""
    if cli_value:
        return cli_value

    if env_val := os.getenv("BLUEPRINT_TEMPLATE_PATH"):
        return env_val

    config = load_config()
    if template_path := config.get("template_path"):
        return template_path

    # Default
    airflow_home = os.getenv("AIRFLOW_HOME", str(Path("~/airflow").expanduser()))
    return str(Path(airflow_home) / "blueprint/templates")


def get_output_dir(cli_value: Optional[str] = None) -> str:
    """Get output dir with precedence: CLI > config > default."""
    if cli_value:
        return cli_value

    config = load_config()
    if output_dir := config.get("output_dir"):
        return output_dir

    # Default using Airflow's dags folder
    return str(get_airflow_dags_folder() / "configs")


def get_airflow_dags_folder() -> Path:
    """Get the dags folder from Airflow configuration.

    Returns:
        Path to the configured dags folder, falling back to AIRFLOW_HOME/dags
    """
    try:
        from airflow.configuration import conf

        dags_folder = conf.get("core", "dags_folder")
        return Path(dags_folder)
    except (ImportError, Exception):
        # Fall back to AIRFLOW_HOME/dags or ~/airflow/dags
        airflow_home = os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))
        return Path(airflow_home) / "dags"
