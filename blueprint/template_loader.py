"""Blueprint template loader utility.

This module handles the configuration and loading of Blueprint templates
from a configurable path, defaulting to $AIRFLOW_HOME/blueprint/templates.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from blueprint.config import get_output_dir
from blueprint.errors import BlueprintError, DuplicateDAGIdError
from blueprint.loaders import from_yaml
from blueprint.utils import get_template_path as utils_get_template_path

# Default template path relative to AIRFLOW_HOME
DEFAULT_TEMPLATE_PATH = "blueprint/templates"

# Create logger for this module
logger = logging.getLogger(__name__)


def _raise_duplicate_error(dag_id: str, conflicting_configs: list) -> None:
    """Raise a DuplicateDAGIdError for the given DAG ID and configs."""
    raise DuplicateDAGIdError(dag_id, conflicting_configs)


# Moved to utils.py to avoid circular import


def get_template_path() -> str:
    """Get the template path from environment or default."""
    return utils_get_template_path()


def setup_template_path() -> None:
    """Add the template path to sys.path if not already present."""
    template_path = get_template_path()

    # Convert to absolute path
    template_path = Path(template_path).resolve()

    # Only add to sys.path if the directory exists and not already there
    if template_path.exists() and str(template_path) not in sys.path:
        sys.path.insert(0, str(template_path))
    # If it doesn't exist, that's okay - we'll handle this gracefully elsewhere


def load_template(module_name: str, class_name: str) -> Any:
    """Dynamically load a template class from a module.

    Args:
        module_name: Name of the module (e.g., 'daily_etl')
        class_name: Name of the class to import (e.g., 'DailyETL')

    Returns:
        The imported class
    """
    setup_template_path()

    try:
        module = __import__(module_name)
        return getattr(module, class_name)
    except ImportError as e:
        template_path = get_template_path()
        error_msg = f"Could not import {class_name} from {module_name}: {e}"
        if not Path(template_path).exists():
            error_msg += f"\nTemplate directory does not exist: {template_path}"
        raise ImportError(error_msg) from e
    except AttributeError as e:
        error_msg = f"Class {class_name} not found in {module_name}: {e}"
        raise AttributeError(error_msg) from e


def discover_yaml_dags(
    configs_dir: Optional[str] = None,
    template_dir: Optional[str] = None,
    pattern: str = "*.dag.yaml",
) -> Dict[str, Any]:
    """Discover and load DAGs from YAML configuration files.

    Args:
        configs_dir: Directory containing YAML config files (defaults to dags/configs/)
        template_dir: Directory containing template files (defaults to blueprint/templates)
        pattern: File pattern to match (defaults to "*.dag.yaml")

    Returns:
        Dictionary mapping DAG names to DAG objects
    """
    # Determine configs directory
    configs_dir_path = Path(get_output_dir() if configs_dir is None else configs_dir)

    # Determine template directory (use registry now)
    template_dir = get_template_path() if template_dir is None else str(template_dir)

    dags = {}
    failed_configs = []
    dag_id_to_configs = {}  # Track DAG IDs to config files for duplicate detection

    logger.info("Discovering DAG configurations in %s (pattern: %s)", configs_dir_path, pattern)

    if not configs_dir_path.exists():
        logger.warning("Configuration directory does not exist: %s", configs_dir_path)
        return dags

    config_files = list(configs_dir_path.rglob(pattern))
    if not config_files:
        logger.warning(
            "No configuration files found matching pattern '%s' in %s",
            pattern,
            configs_dir_path,
        )
        return dags

    logger.info("Found %d configuration files", len(config_files))

    for yaml_file in config_files:
        try:
            # Load the DAG from YAML (now uses registry automatically)
            dag = from_yaml(str(yaml_file), template_dir=template_dir)

            # Check for duplicate DAG IDs
            dag_id = dag.dag_id
            if dag_id in dag_id_to_configs:
                # Duplicate found - collect all files with this DAG ID
                conflicting_configs = dag_id_to_configs[dag_id] + [yaml_file]
                _raise_duplicate_error(dag_id, conflicting_configs)

            # Track this DAG ID
            dag_id_to_configs[dag_id] = [yaml_file]

            # Use the filename (without extension) as the key
            dag_name = yaml_file.stem.replace(".dag", "")
            dags[dag_name] = dag

            logger.info("✅ Loaded DAG from %s: %s", yaml_file.name, dag.dag_id)

        except DuplicateDAGIdError:
            # Re-raise duplicate DAG ID errors immediately - they should stop processing
            raise
        except BlueprintError as e:
            # Our rich errors - log the formatted message
            failed_configs.append((yaml_file, e))
            logger.exception("❌ Failed to load %s:", yaml_file.name)

        except Exception as e:
            # Unexpected errors
            failed_configs.append((yaml_file, e))
            logger.exception("❌ Failed to load %s:", yaml_file.name)

    # Summary logging
    if failed_configs:
        logger.warning(
            "Failed to load %d of %d DAG configurations",
            len(failed_configs),
            len(config_files),
        )
        logger.info("Failed configurations:")
        for yaml_file, error in failed_configs:
            logger.info("  • %s: %s", yaml_file.name, type(error).__name__)

    if dags:
        logger.info("Successfully loaded %d DAGs: %s", len(dags), ", ".join(sorted(dags.keys())))
    else:
        logger.warning("No DAGs were successfully loaded")

    return dags


def auto_load_yaml_dags(
    configs_dir: Optional[str] = None,
    template_dir: Optional[str] = None,
    pattern: str = "*.dag.yaml",
) -> None:
    """Automatically discover and register DAGs from YAML files.

    This function loads DAGs from YAML files and registers them in the global namespace
    so Airflow can discover them. It's designed to be called from a DAG file.

    Args:
        configs_dir: Directory containing YAML config files
        template_dir: Directory containing template files
        pattern: File pattern to match
    """
    dags = discover_yaml_dags(configs_dir, template_dir, pattern)

    # Register DAGs in globals
    for dag_name, dag in dags.items():
        globals()[f"{dag_name}_dag"] = dag


# Auto-setup when module is imported
setup_template_path()
