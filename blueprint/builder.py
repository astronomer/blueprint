"""DAG builder that composes Blueprint instances into Airflow DAGs."""

import inspect
import logging
from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from blueprint.core import TaskOrGroup
from blueprint.errors import (
    ConfigurationError,
    CyclicDependencyError,
    DuplicateDAGIdError,
    InvalidDependencyError,
)
from blueprint.registry import BlueprintRegistry, registry

if TYPE_CHECKING:
    from airflow import DAG

logger = logging.getLogger(__name__)

OnDagBuilt = Callable[["DAG", Path], None]

DEFAULT_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

_PARAM_SCHEMA_KEYS = frozenset(
    {
        "type",
        "enum",
        "const",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "pattern",
        "items",
        "minItems",
        "maxItems",
        "properties",
        "required",
        "additionalProperties",
        "anyOf",
        "format",
        "examples",
        "values_display",
        "description_md",
    }
)


def _config_to_params(
    config_type: type[BaseModel],
    config_values: dict[str, Any],
    step_name: str,
) -> dict[str, Any]:
    """Convert a Pydantic config model and YAML values to Airflow Param objects.

    Generates one Param per config field, namespaced as ``{step_name}__{field}``.
    YAML values are used as defaults; JSON Schema properties from the Pydantic
    model drive type validation and Airflow trigger form rendering.

    Args:
        config_type: The Pydantic config model class.
        config_values: The YAML-provided values for this step.
        step_name: The step name used for param namespacing and UI sections.

    Returns:
        Dict mapping ``{step_name}__{field_name}`` to Airflow Param objects.
    """
    from airflow.models.param import Param

    from blueprint.core import _resolve_refs

    schema = _resolve_refs(config_type.model_json_schema())
    properties = schema.get("properties", {})
    params: dict[str, Any] = {}

    for field_name, field_schema in properties.items():
        default = config_values.get(field_name, field_schema.get("default"))

        description = field_schema.get("description") or field_schema.get("title")

        param_kwargs: dict[str, Any] = {}
        for key in _PARAM_SCHEMA_KEYS:
            if key in field_schema:
                param_kwargs[key] = field_schema[key]

        param_kwargs["section"] = step_name

        param_key = f"{step_name}__{field_name}"
        params[param_key] = Param(
            default=default,
            description=description,
            **param_kwargs,
        )

    return params


class StepConfig(BaseModel):
    """Configuration for a single step in a DAG."""

    model_config = ConfigDict(extra="allow")

    blueprint: str
    depends_on: list[str] = Field(default_factory=list)
    version: int | None = None

    def get_blueprint_config(self) -> dict[str, Any]:
        """Get the blueprint-specific config (everything except reserved keys)."""
        extra = self.model_extra or {}
        return dict(extra)


class DAGConfig(BaseModel):
    """Top-level DAG configuration parsed from YAML."""

    model_config = ConfigDict(extra="allow")

    dag_id: str
    steps: dict[str, StepConfig]

    @model_validator(mode="after")
    def validate_steps_not_empty(self) -> "DAGConfig":
        if not self.steps:
            msg = "DAG must have at least one step"
            raise ValueError(msg)
        return self

    def get_extra_fields(self) -> dict[str, Any]:
        """Get all fields beyond dag_id and steps (dag args config values)."""
        return dict(self.model_extra or {})


class Builder:
    """Builds Airflow DAGs from YAML-defined step compositions.

    The builder resolves blueprint classes from the registry, validates
    step configs against their Pydantic models, renders tasks/TaskGroups,
    and wires up dependencies.
    """

    def __init__(
        self,
        bp_registry: BlueprintRegistry | None = None,
        on_dag_built: OnDagBuilt | None = None,
    ) -> None:
        self._registry = bp_registry or registry
        self._on_dag_built = on_dag_built

    def build(self, config: DAGConfig) -> "DAG":
        """Build a DAG from a DAGConfig.

        Args:
            config: The parsed and validated DAG configuration

        Returns:
            A fully wired Airflow DAG
        """
        from airflow import DAG

        self.validate_dependencies(config)

        dag_args_cls = self._registry.get_dag_args()
        config_type = dag_args_cls.get_config_type()
        extra = config.get_extra_fields()
        validated_dag_args = config_type(**extra)

        instance = dag_args_cls()
        dag_kwargs = instance.render(validated_dag_args)

        if "params" in dag_kwargs:
            msg = (
                "BlueprintDagArgs must not return 'params' — "
                "DAG params are auto-generated from blueprint step configs"
            )
            raise ConfigurationError(msg)

        dag_kwargs["dag_id"] = config.dag_id
        if "start_date" not in dag_kwargs:
            dag_kwargs["start_date"] = DEFAULT_START_DATE
        elif isinstance(dag_kwargs["start_date"], str):
            try:
                parsed = datetime.fromisoformat(dag_kwargs["start_date"])
            except ValueError as e:
                msg = f"Invalid start_date format: {dag_kwargs['start_date']!r}"
                raise ConfigurationError(msg) from e
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            dag_kwargs["start_date"] = parsed
        if "catchup" not in dag_kwargs:
            dag_kwargs["catchup"] = False

        dag_params: dict[str, Any] = {}
        for step_name, step_config in config.steps.items():
            bp_class = self._registry.get(step_config.blueprint, step_config.version)
            if not bp_class.supports_params:
                continue
            ct = bp_class.get_config_type()
            dag_params.update(_config_to_params(ct, step_config.get_blueprint_config(), step_name))
        if dag_params:
            dag_kwargs["params"] = dag_params

        dag = DAG(**dag_kwargs)

        rendered: dict[str, TaskOrGroup] = {}

        with dag:
            for step_name, step_config in config.steps.items():
                rendered[step_name] = self._render_step(step_name, step_config)

            for step_name, step_config in config.steps.items():
                for dep_name in step_config.depends_on:
                    rendered[dep_name] >> rendered[step_name]

        return dag

    def build_from_yaml(
        self,
        path: str | Path,
        render_template: bool = True,
        template_context: dict[str, Any] | None = None,
    ) -> "DAG":
        """Load a YAML file and build a DAG.

        Args:
            path: Path to the .dag.yaml file
            render_template: Whether to render Jinja2 templates
            template_context: Additional Jinja2 context

        Returns:
            The built Airflow DAG
        """
        from blueprint.loaders import render_yaml_template

        yaml_path = Path(path)

        if render_template:
            raw_config, _rendered_yaml = render_yaml_template(
                yaml_path, context=template_context, use_airflow_context=True
            )
        else:
            raw_content = yaml_path.read_text()
            raw_config = yaml.safe_load(raw_content)

        dag_config = DAGConfig.model_validate(raw_config)
        dag = self.build(dag_config)

        if self._on_dag_built:
            self._on_dag_built(dag, yaml_path)

        return dag

    def validate_dependencies(self, config: DAGConfig) -> None:
        """Validate that all dependency references exist and detect cycles."""
        step_names = set(config.steps.keys())

        for step_name, step_config in config.steps.items():
            for dep in step_config.depends_on:
                if dep not in step_names:
                    raise InvalidDependencyError(step_name, dep, list(step_names - {step_name}))

        self._detect_cycles(config)

    def _detect_cycles(self, config: DAGConfig) -> None:
        """Detect cycles in the dependency graph using Kahn's algorithm."""
        in_degree: dict[str, int] = {name: 0 for name in config.steps}
        adjacency: dict[str, list[str]] = {name: [] for name in config.steps}

        for step_name, step_config in config.steps.items():
            for dep in set(step_config.depends_on):
                adjacency[dep].append(step_name)
                in_degree[step_name] += 1

        queue: deque[str] = deque()
        for name, degree in in_degree.items():
            if degree == 0:
                queue.append(name)

        visited_count = 0
        while queue:
            node = queue.popleft()
            visited_count += 1
            for neighbor in adjacency[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited_count != len(config.steps):
            cycle = self._find_cycle(config)
            raise CyclicDependencyError(cycle)

    def _find_cycle(self, config: DAGConfig) -> list[str]:
        """Find and return one cycle in the dependency graph for error reporting."""
        _white, _gray, _black = 0, 1, 2
        color: dict[str, int] = {name: _white for name in config.steps}
        path: list[str] = []

        adjacency: dict[str, list[str]] = {name: [] for name in config.steps}
        for step_name, step_config in config.steps.items():
            for dep in step_config.depends_on:
                adjacency[dep].append(step_name)

        def dfs(node: str) -> list[str] | None:
            color[node] = _gray
            path.append(node)
            for neighbor in adjacency[node]:
                if color[neighbor] == _gray:
                    cycle_start = path.index(neighbor)
                    return [*path[cycle_start:], neighbor]
                if color[neighbor] == _white:
                    result = dfs(neighbor)
                    if result:
                        return result
            path.pop()
            color[node] = _black
            return None

        for name in config.steps:
            if color[name] == _white:
                result = dfs(name)
                if result:
                    return result

        msg = "Cycle detected by topological sort but DFS failed to locate it"
        raise AssertionError(msg)

    def _render_step(
        self,
        step_name: str,
        step_config: StepConfig,
    ) -> TaskOrGroup:
        """Render a single step by instantiating its blueprint."""
        from pydantic import ValidationError

        bp_class = self._registry.get(step_config.blueprint, step_config.version)

        config_type = bp_class.get_config_type()
        blueprint_config = step_config.get_blueprint_config()

        resolved_version = step_config.version
        if resolved_version is None:
            resolved_version = self._registry.get_latest_version(step_config.blueprint)

        try:
            validated_config = config_type(**blueprint_config)
        except ValidationError as e:
            error_lines = []
            for err in e.errors():
                field = " -> ".join(str(loc) for loc in err["loc"])
                err_type = err["type"]
                if err_type == "missing":
                    error_lines.append(f"  - missing required field '{field}'")
                elif err_type == "extra_forbidden":
                    error_lines.append(f"  - unexpected field '{field}'")
                elif err_type == "string_pattern_mismatch":
                    pattern = err.get("ctx", {}).get("pattern", "")
                    error_lines.append(
                        f"  - '{field}': must match pattern '{pattern}', got {err.get('input')!r}"
                    )
                else:
                    error_lines.append(f"  - '{field}': {err['msg']}")

            unknown = set(blueprint_config) - set(config_type.model_fields)
            for field_name in sorted(unknown):
                error_lines.append(f"  - unexpected field '{field_name}'")

            errors_str = "\n".join(error_lines)

            msg = (
                f"Step '{step_name}' has invalid config for blueprint "
                f"'{step_config.blueprint}' (v{resolved_version}, {config_type.__name__}):\n"
                f"{errors_str}\n\n"
                f"Run 'blueprint lint' to validate your DAG files, or "
                f"'blueprint describe {step_config.blueprint}' to see the expected config."
            )
            raise ConfigurationError(msg) from e

        instance = bp_class()
        instance.step_id = step_name

        result = instance.render(validated_config)

        step_yaml = yaml.dump(
            {
                "blueprint": step_config.blueprint,
                "version": resolved_version,
                **blueprint_config,
            },
            default_flow_style=False,
            sort_keys=False,
        )

        source_code = bp_class.get_source_code()

        self._inject_step_context(result, step_yaml, source_code)

        return result

    def _inject_step_context(
        self,
        rendered: TaskOrGroup,
        step_yaml: str,
        source_code: str,
    ) -> None:
        """Inject step config and blueprint source into all tasks for Airflow UI visibility."""
        from airflow.models import BaseOperator
        from airflow.utils.task_group import TaskGroup

        def _collect_operators(node: TaskOrGroup) -> list[BaseOperator]:
            if isinstance(node, TaskGroup):
                ops: list[BaseOperator] = []
                for child in node.children.values():
                    if isinstance(child, BaseOperator):
                        ops.append(child)
                    elif isinstance(child, TaskGroup):
                        ops.extend(_collect_operators(child))
                return ops
            if isinstance(node, BaseOperator):
                return [node]
            return []

        tasks = _collect_operators(rendered)
        if not tasks:
            return

        for task in tasks:
            task.blueprint_step_config = step_yaml  # type: ignore[attr-defined]
            task.blueprint_step_code = source_code  # type: ignore[attr-defined]

            existing_fields = getattr(task, "template_fields", ()) or ()
            new_fields = []
            for field_name in ("blueprint_step_config", "blueprint_step_code"):
                if field_name not in existing_fields:
                    new_fields.append(field_name)
            if new_fields:
                task.template_fields = (*existing_fields, *new_fields)

            task.template_fields_renderers = {  # type: ignore[attr-defined]
                **getattr(task, "template_fields_renderers", {}),
                "blueprint_step_config": "yaml",
                "blueprint_step_code": "py",
            }


def _check_duplicate_dag_id(dag_id: str, yaml_path: Path, dag_id_to_file: dict[str, Path]) -> None:
    """Check for duplicate DAG IDs and raise if found."""
    if dag_id in dag_id_to_file:
        raise DuplicateDAGIdError(dag_id, [dag_id_to_file[dag_id], yaml_path])


def build_all(
    search_path: str | Path | None = None,
    register_globals: dict | None = None,
    pattern: str = "*.dag.yaml",
    render_templates: bool = True,
    template_context: dict[str, Any] | None = None,
    bp_registry: BlueprintRegistry | None = None,
    on_dag_built: OnDagBuilt | None = None,
) -> list["DAG"]:
    """Discover and build all DAGs from YAML files.

    This is the top-level convenience function meant to be called from a
    DAG loader file (e.g., loader.py in your dags/ directory).

    Args:
        search_path: Directory to search for YAML files. Defaults to dags/
            or the directory containing the calling file.
        register_globals: Dict to register DAGs in. If not provided,
            automatically uses the caller's globals().
        pattern: Glob pattern for YAML discovery (default: *.dag.yaml)
        render_templates: Whether to render Jinja2 templates in YAML files
        template_context: Additional context variables for template rendering
        bp_registry: Custom BlueprintRegistry to use. If not provided,
            auto-discovers blueprints from the search path.
        on_dag_built: Optional callback invoked after each DAG is built.
            Receives the DAG and the Path to the source YAML file.
            Use this to apply post-processing such as access controls or tags.

    Returns:
        List of built DAGs

    Example:
        ```python
        # In dags/loader.py
        from blueprint import build_all

        build_all()
        ```
    """
    from blueprint.loaders import render_yaml_template

    if register_globals is None:
        frame = inspect.currentframe()
        register_globals = frame.f_back.f_globals if frame and frame.f_back else {}

    resolved_path = _resolve_search_path(search_path)

    if bp_registry is None:
        caller_file = _get_caller_file()
        exclude = {Path(caller_file)} if caller_file else set()
        bp_registry = BlueprintRegistry(template_dirs=[resolved_path], exclude_files=exclude)
        bp_registry.discover(force=True)

    builder = Builder(bp_registry=bp_registry)

    logger.info("Discovering DAGs in %s (pattern: %s)", resolved_path, pattern)

    yaml_files = list(resolved_path.rglob(pattern)) if resolved_path.exists() else []
    if not yaml_files:
        logger.debug("No YAML files found matching '%s' in %s", pattern, resolved_path)
        return []

    dags: list[DAG] = []
    dag_id_to_file: dict[str, Path] = {}

    for yaml_path in yaml_files:
        if render_templates:
            raw_config, _rendered = render_yaml_template(
                yaml_path, context=template_context, use_airflow_context=True
            )
        else:
            raw_content = yaml_path.read_text()
            raw_config = yaml.safe_load(raw_content)

        if not raw_config or "steps" not in raw_config:
            logger.debug("Skipping %s: no 'steps' field", yaml_path.name)
            continue

        dag_config = DAGConfig.model_validate(raw_config)
        _check_duplicate_dag_id(dag_config.dag_id, yaml_path, dag_id_to_file)
        dag = builder.build(dag_config)

        if on_dag_built:
            on_dag_built(dag, yaml_path)

        dag_id_to_file[dag.dag_id] = yaml_path
        register_globals[dag.dag_id] = dag
        dags.append(dag)
        logger.info("Built DAG '%s' from %s", dag.dag_id, yaml_path.name)

    if dags:
        logger.info(
            "Successfully built %d DAG(s): %s",
            len(dags),
            ", ".join(d.dag_id for d in dags),
        )

    return dags


def _get_caller_file() -> str | None:
    """Return the __file__ of the module that called build_all().

    Walks the call stack to find the first frame outside of the blueprint
    package, making this resilient to internal helper wrappers.
    """
    frame = inspect.currentframe()
    try:
        blueprint_pkg = str(Path(__file__).parent.resolve())
        current = frame
        while current is not None:
            current = current.f_back
            if current is None:
                break
            caller = current.f_globals.get("__file__")
            if caller and not str(Path(caller).resolve()).startswith(blueprint_pkg):
                return caller
        return None
    finally:
        del frame


def _resolve_search_path(search_path: str | Path | None) -> Path:
    """Resolve the search path for YAML discovery.

    Resolution order:
    1. Explicit search_path argument
    2. Directory of the file that called build_all()
    3. Current working directory
    """
    if search_path is not None:
        return Path(search_path)

    caller_file = _get_caller_file()
    if caller_file:
        return Path(caller_file).parent

    return Path.cwd()
