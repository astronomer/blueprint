"""YAML loading and blueprint discovery functionality."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment

from blueprint.airflow_context import get_airflow_template_context
from blueprint.core import Blueprint
from blueprint.errors import ConfigurationError, YAMLParseError
from blueprint.registry import BlueprintRegistry, registry


def render_yaml_template(
    path: Union[str, Path],
    context: Optional[Dict[str, Any]] = None,
    use_airflow_context: bool = True,
) -> Tuple[Dict[str, Any], str]:
    """Render a Jinja2-templated YAML file.

    Args:
        path: Path to YAML template file
        context: Additional context variables for template rendering
        use_airflow_context: Include Airflow vars/conns in context (default: True)

    Returns:
        Tuple of (parsed config dict, rendered YAML string)

    Raises:
        YAMLParseError: If YAML parsing fails
        ConfigurationError: If template rendering fails

    Example:
        ```python
        config, rendered = render_yaml_template("dags/customer_etl.dag.yaml")
        # config is the parsed dict
        # rendered is the string with all Jinja2 templates resolved
        ```
    """
    config_path = Path(path)

    # Read the raw template content
    try:
        raw_content = config_path.read_text()
    except Exception as e:
        msg = f"Failed to read configuration file: {e}"
        raise ConfigurationError(msg, config_path) from e

    # Build the template context
    template_context: Dict[str, Any] = {}
    if use_airflow_context:
        template_context = get_airflow_template_context()
    if context:
        template_context.update(context)

    # Render the Jinja2 template
    try:
        env = SandboxedEnvironment(undefined=StrictUndefined)
        # Add default filter for fallback values
        env.filters["default"] = lambda value, default="": value if value else default
        template = env.from_string(raw_content)
        rendered_content = template.render(template_context)
    except TemplateError as e:
        msg = f"Template rendering failed: {e}"
        raise ConfigurationError(
            msg,
            config_path,
            suggestions=[
                "Check that all template variables are defined",
                "Verify Jinja2 syntax is correct",
                "Use {{ env.VAR_NAME }} for environment variables",
                "Use {{ var.value.key }} for Airflow Variables",
            ],
        ) from e

    # Parse the rendered YAML
    try:
        config = yaml.safe_load(rendered_content)
    except yaml.YAMLError as e:
        raise YAMLParseError.from_yaml_error(e, config_path) from e

    if not config:
        msg = "Configuration file is empty after rendering"
        raise ConfigurationError(
            msg,
            config_path,
            suggestions=[
                "Add a 'blueprint' field to specify which blueprint to use",
                "Add configuration parameters for your blueprint",
            ],
        )

    return config, rendered_content


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
    render_template: bool = True,
    template_context: Optional[Dict[str, Any]] = None,
):
    """Load a blueprint from YAML configuration.

    Args:
        path: Path to YAML configuration file
        overrides: Optional parameter overrides
        template_dir: Directory containing blueprint templates
        validate_only: If True, only validate config without rendering DAG
        render_template: If True, render Jinja2 templates in the YAML file (default: True)
        template_context: Additional context variables for template rendering

    Returns:
        The rendered DAG (or validated config if validate_only=True)

    Example:
        ```python
        # Load and render a DAG with Jinja2 templating
        dag = from_yaml("dags/customer_etl.dag.yaml", overrides={"retries": 5})

        # Load without template rendering
        dag = from_yaml("dags/static_config.dag.yaml", render_template=False)

        # Just validate the configuration
        config = from_yaml("dags/test.dag.yaml", validate_only=True)
        ```
    """
    config_path = Path(path)

    # Load YAML configuration with optional Jinja2 rendering
    if render_template:
        config, rendered_yaml = render_yaml_template(
            config_path,
            context=template_context,
            use_airflow_context=True,
        )
    else:
        # Load without template rendering
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
