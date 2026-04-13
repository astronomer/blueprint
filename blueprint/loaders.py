"""YAML loading, Jinja2 rendering, and blueprint discovery for DAG definitions."""

import logging
from pathlib import Path
from typing import Any

import yaml

from blueprint.core import Blueprint
from blueprint.errors import ConfigurationError, YAMLParseError
from blueprint.registry import BlueprintRegistry, registry

logger = logging.getLogger(__name__)


class _ContextProxy:
    """Proxy that generates Airflow runtime template expressions.

    Attribute access, item access, and function calls build up an expression
    string. When Jinja2 converts the result to a string, it produces
    ``{{ expression }}`` — a literal Airflow template macro that Airflow
    resolves at task execution time.
    """

    def __init__(self, expr: str = "") -> None:
        object.__setattr__(self, "_expr", expr)

    def __getattr__(self, name: str) -> "_ContextProxy":
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        new_expr = f"{self._expr}.{name}" if self._expr else name
        return _ContextProxy(new_expr)

    def __getitem__(self, key: object) -> "_ContextProxy":
        return _ContextProxy(f"{self._expr}[{key!r}]")

    def __call__(self, *args: object, **kwargs: object) -> "_ContextProxy":
        parts = [repr(a) for a in args]
        parts.extend(f"{k}={v!r}" for k, v in kwargs.items())
        return _ContextProxy(f"{self._expr}({', '.join(parts)})")

    def __str__(self) -> str:
        return "{{ " + self._expr + " }}"

    def __repr__(self) -> str:
        return f"_ContextProxy({self._expr!r})"

    def __bool__(self) -> bool:
        return True


class _StubVarAccessor:
    """Stub Variable accessor for use outside Airflow (e.g. lint).

    ``var.get(key, default)`` always returns the default.
    ``var.value.key`` returns a placeholder string.
    """

    @property
    def value(self) -> "_StubVarAccessor":
        return self

    def __getattr__(self, name: str) -> str:
        return f"<{name}>"

    def get(self, key: str, default: str | None = None) -> str | None:  # noqa: ARG002
        return default


def render_yaml_template(
    path: Path,
    context: dict[str, Any] | None = None,
    use_airflow_context: bool = True,
) -> tuple[dict[str, Any], str]:
    """Load a YAML file and optionally render Jinja2 templates.

    Args:
        path: Path to the YAML file
        context: Additional template context
        use_airflow_context: Whether to include Airflow env/var/conn context

    Returns:
        Tuple of (parsed config dict, rendered YAML string)
    """
    try:
        raw_content = path.read_text()
    except OSError as e:
        msg = f"Failed to read file: {e}"
        raise ConfigurationError(msg, path) from e

    rendered_content = raw_content
    has_jinja = "{{" in raw_content or "{%" in raw_content

    if has_jinja:
        try:
            import os

            import jinja2

            template_context: dict[str, Any] = {
                "context": _ContextProxy(),
                "env": os.environ,
                "var": _StubVarAccessor(),
            }
            if use_airflow_context:
                template_context.update(_get_airflow_context())
            if context:
                template_context.update(context)

            env = jinja2.Environment(undefined=jinja2.StrictUndefined)
            template = env.from_string(raw_content)
            rendered_content = template.render(**template_context)
        except ImportError:
            logger.debug("Jinja2 not available, skipping template rendering")
        except jinja2.TemplateError as e:
            msg = f"Jinja2 template rendering failed: {e}"
            raise ConfigurationError(msg, path) from e

    try:
        config = yaml.safe_load(rendered_content)
    except yaml.YAMLError as e:
        raise YAMLParseError.from_yaml_error(e, path) from e

    if not config or not isinstance(config, dict):
        msg = "Configuration file is empty or invalid"
        raise ConfigurationError(
            msg,
            path,
            suggestions=[
                "Ensure the YAML file contains valid configuration",
                "The file must define dag_id and steps",
            ],
        )

    return config, rendered_content


def _get_airflow_context() -> dict[str, Any]:
    """Build Jinja2 context with Airflow variables, connections, and env."""
    import os

    ctx: dict[str, Any] = {"env": os.environ}

    try:
        from airflow.models import Variable

        class VarAccessor:
            """Provides access to Airflow Variables in Jinja2 templates."""

            @property
            def value(self):
                return self

            def __getattr__(self, name: str) -> str:
                return Variable.get(name)

            def get(self, key: str, default: str | None = None) -> str | None:
                try:
                    return Variable.get(key)
                except Exception:
                    return default

        ctx["var"] = VarAccessor()
    except ImportError:
        pass

    try:
        from airflow.hooks.base import BaseHook

        class ConnAccessor:
            """Provides access to Airflow Connections in Jinja2 templates."""

            def get(self, conn_id: str) -> Any:
                return BaseHook.get_connection(conn_id)

        ctx["conn"] = ConnAccessor()
    except ImportError:
        pass

    return ctx


def load_blueprint(
    blueprint_name: str,
    template_dir: str | None = None,
    version: int | None = None,
) -> type[Blueprint]:
    """Load a blueprint class by name and optional version.

    Args:
        blueprint_name: Name of the blueprint (e.g., 'extract')
        template_dir: Directory containing blueprint files
        version: Specific version (None for latest)

    Returns:
        The Blueprint class
    """
    reg = get_registry(template_dir)
    return reg.get(blueprint_name, version)


def discover_blueprints(template_dir: str | None = None) -> list[dict[str, Any]]:
    """Discover all available blueprints.

    Args:
        template_dir: Directory containing blueprint files

    Returns:
        List of blueprint information dictionaries
    """
    reg = get_registry(template_dir)
    return reg.list_blueprints()


def get_blueprint_info(
    blueprint_name: str,
    template_dir: str | None = None,
    version: int | None = None,
) -> dict[str, Any]:
    """Get detailed information about a specific blueprint.

    Args:
        blueprint_name: Name of the blueprint
        template_dir: Directory containing blueprint files
        version: Specific version (None for latest)

    Returns:
        Dictionary with blueprint information including schema
    """
    reg = get_registry(template_dir)
    return reg.get_blueprint_info(blueprint_name, version)


def validate_yaml(
    path: str,
    template_dir: str | None = None,
) -> dict[str, Any]:
    """Validate a DAG YAML file without building the DAG.

    Args:
        path: Path to the .dag.yaml file
        template_dir: Directory containing blueprint files

    Returns:
        The parsed and validated DAGConfig as a dict
    """
    from blueprint.builder import Builder, DAGConfig

    config_path = Path(path)
    config, _rendered = render_yaml_template(config_path, use_airflow_context=False)

    dag_config = DAGConfig.model_validate(config)

    reg = get_registry(template_dir)

    builder = Builder(bp_registry=reg)
    builder.validate_dependencies(dag_config)

    dag_args_cls = reg.get_dag_args()
    dag_args_config_type = dag_args_cls.get_config_type()
    dag_args_config_type(**dag_config.get_extra_fields())

    for _step_name, step_config in dag_config.steps.items():
        bp_class = reg.get(step_config.blueprint, step_config.version)
        config_type = bp_class.get_config_type()
        blueprint_config = step_config.get_blueprint_config()
        config_type(**blueprint_config)

    return dag_config.model_dump()


def get_registry(template_dir: str | None = None) -> BlueprintRegistry:
    """Get or create a BlueprintRegistry for the given template directory."""
    if template_dir:
        temp_registry = BlueprintRegistry(template_dirs=[Path(template_dir)])
        temp_registry.discover(force=True)
        return temp_registry

    registry.discover()
    return registry
