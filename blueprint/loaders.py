"""YAML loading and blueprint discovery functionality."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from blueprint import Blueprint
from blueprint.errors import ConfigurationError, YAMLParseError
from blueprint.registry import BlueprintRegistry, registry


def load_blueprint(blueprint_name: str, template_dir: Optional[str] = None) -> type[Blueprint]:
    """Load a blueprint class by name.

    Args:
        blueprint_name: Name of the blueprint (e.g., 'daily_etl')
        template_dir: Directory containing blueprint templates

    Returns:
        The Blueprint class

    Raises:
        BlueprintNotFoundError: If blueprint not found
        DuplicateBlueprintError: If multiple blueprints with same name
    """
    if template_dir:
        # Create a temporary registry for the specific template directory
        temp_registry = BlueprintRegistry()

        # Override the get_template_dirs method to use the provided directory
        temp_registry.get_template_dirs = lambda: [Path(template_dir)]  # type: ignore[method-assign]

        # Discover blueprints in the specified directory
        temp_registry.discover_blueprints(force=True)

        return temp_registry.get_blueprint(blueprint_name)
    # Use the global registry
    registry.discover_blueprints()
    return registry.get_blueprint(blueprint_name)


def from_yaml(
    path: str,
    overrides: Optional[Dict[str, Any]] = None,
    template_dir: Optional[str] = None,
    validate_only: bool = False,
):
    """Load a blueprint from YAML configuration.

    Args:
        path: Path to YAML configuration file
        overrides: Optional parameter overrides
        template_dir: Directory containing blueprint templates (deprecated - use BLUEPRINT_TEMPLATE_PATH env var)
        validate_only: If True, only validate config without rendering DAG

    Returns:
        The rendered DAG (or validated config if validate_only=True)

    Example:
        ```python
        # Load and render a DAG
        dag = from_yaml("configs/customer_etl.yaml", overrides={"retries": 5})

        # Just validate the configuration
        config = from_yaml("configs/test.yaml", validate_only=True)
        ```
    """
    config_path = Path(path)

    # Load YAML configuration with error handling
    try:
        with config_path.open() as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise YAMLParseError.from_yaml_error(e, config_path) from e
    except Exception as e:
        msg = f"Failed to read configuration file: {e}"
        raise ConfigurationError(msg, config_path) from e

    if not config:
        msg = "Configuration file is empty"
        raise ConfigurationError(
            msg,
            config_path,
            suggestions=[
                "Add a 'blueprint' field to specify which blueprint to use",
                "Add configuration parameters for your blueprint",
            ],
        )

    # Extract blueprint name
    blueprint_name = config.pop("blueprint", None)
    if not blueprint_name:
        msg = "Missing required field 'blueprint'"
        raise ConfigurationError(
            msg,
            config_path,
            suggestions=[
                "Add 'blueprint: <blueprint_name>' to your configuration",
                "Use 'blueprint list' to see available blueprints",
            ],
        )

    # Apply overrides
    if overrides:
        config.update(overrides)

    # Load blueprint class
    try:
        blueprint_class = load_blueprint(blueprint_name, template_dir)

        if validate_only:
            # Just validate the config using Pydantic - no DAG rendering
            config_type = blueprint_class.get_config_type()
            return config_type(**config)  # This runs all Pydantic validation
        # Full build including DAG rendering
        return blueprint_class.build(**config)

    except Exception as e:
        # Enhance error with configuration context
        if "ValidationError" in str(type(e)):
            msg = f"Configuration validation failed: {e}"
            raise ConfigurationError(
                msg,
                config_path,
                suggestions=[
                    "Check that all required parameters are provided",
                    "Verify parameter types match the blueprint requirements",
                    f"Use 'blueprint describe {blueprint_name}' to see parameter details",
                ],
            ) from e
        raise


def discover_blueprints(template_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """Discover all available blueprints.

    Args:
        template_dir: Directory containing blueprint templates

    Returns:
        List of blueprint information dictionaries
    """
    if template_dir:
        # Create a temporary registry for the specific template directory
        temp_registry = BlueprintRegistry()

        # Override the get_template_dirs method to use the provided directory
        temp_registry.get_template_dirs = lambda: [Path(template_dir)]  # type: ignore[method-assign]

        # Discover blueprints in the specified directory
        temp_registry.discover_blueprints(force=True)

        return temp_registry.list_blueprints()
    # Use the global registry
    registry.discover_blueprints()
    return registry.list_blueprints()


def get_blueprint_info(blueprint_name: str, template_dir: Optional[str] = None) -> Dict[str, Any]:
    """Get detailed information about a specific blueprint.

    Args:
        blueprint_name: Name of the blueprint
        template_dir: Directory containing blueprint templates

    Returns:
        Dictionary with blueprint information including schema
    """
    if template_dir:
        # Create a temporary registry for the specific template directory
        temp_registry = BlueprintRegistry()

        # Override the get_template_dirs method to use the provided directory
        temp_registry.get_template_dirs = lambda: [Path(template_dir)]  # type: ignore[method-assign]

        # Discover blueprints in the specified directory
        temp_registry.discover_blueprints(force=True)

        return temp_registry.get_blueprint_info(blueprint_name)
    # Use the global registry
    registry.discover_blueprints()
    return registry.get_blueprint_info(blueprint_name)
