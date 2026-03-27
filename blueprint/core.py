"""Core Blueprint base class for reusable task templates."""

import copy
import inspect
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.task_group import TaskGroup

T = TypeVar("T", bound=BaseModel)

TaskOrGroup = Union["BaseOperator", "TaskGroup"]

_YAML_SCALAR_TYPES: set[type] = {str, int, float, bool}


def _check_literal_args(args: tuple) -> str | None:
    """Check that all Literal values are YAML-compatible scalars."""
    for val in args:
        if not isinstance(val, str | int | float | bool):
            return f"Literal value {val!r} is not a YAML-compatible type"
    return None


def _check_dict_args(args: tuple, seen: set[int]) -> str | None:
    """Check dict type args: keys must be str, values must be YAML-compatible."""
    if not args:
        return None
    key_type, value_type = args
    if key_type is not str:
        return f"dict key type must be str, got {key_type}"
    return _is_yaml_compatible(value_type, seen)


def _check_concrete_type(annotation: type, seen: set[int]) -> str | None:
    """Check a concrete (non-generic) type for YAML compatibility."""
    if annotation in _YAML_SCALAR_TYPES:
        return None
    if issubclass(annotation, BaseModel):
        model_id = id(annotation)
        if model_id in seen:
            return None
        seen.add(model_id)
        for field_name, field_info in annotation.model_fields.items():
            error = _is_yaml_compatible(field_info.annotation, seen)
            if error:
                return f"field '{field_name}': {error}"
        return None
    return f"type {annotation.__name__} is not YAML-compatible"


def _is_yaml_compatible(annotation: Any, seen: set[int] | None = None) -> str | None:  # noqa: PLR0911
    """Check if a type annotation is YAML-compatible.

    Returns None if compatible, or an error message string if not.
    """
    import types

    if seen is None:
        seen = set()

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Union or isinstance(annotation, types.UnionType):
        for arg in args or get_args(annotation):
            if arg is type(None):
                continue
            error = _is_yaml_compatible(arg, seen)
            if error:
                return error
        return None

    if origin is Literal:
        return _check_literal_args(args)
    if origin is list:
        return _is_yaml_compatible(args[0], seen) if args else None
    if origin is dict:
        return _check_dict_args(args, seen)
    if isinstance(annotation, type):
        return _check_concrete_type(annotation, seen)
    return f"type {annotation!r} is not YAML-compatible"


def _resolve_refs(schema: dict) -> dict:
    """Resolve all $ref/$defs in a JSON Schema, inlining definitions."""
    defs = schema.get("$defs", {})
    if not defs:
        result = copy.deepcopy(schema)
        result.pop("$defs", None)
        return result

    def _resolve(node: Any, resolving: set[str] | None = None) -> Any:
        if resolving is None:
            resolving = set()

        if isinstance(node, dict):
            if "$ref" in node and len(node) == 1:
                ref_path = node["$ref"]
                prefix = "#/$defs/"
                if ref_path.startswith(prefix):
                    def_name = ref_path[len(prefix) :]
                    if def_name in resolving:
                        return copy.deepcopy(node)
                    if def_name in defs:
                        resolving = resolving | {def_name}
                        return _resolve(copy.deepcopy(defs[def_name]), resolving)
                return copy.deepcopy(node)
            return {k: _resolve(v, resolving) for k, v in node.items() if k != "$defs"}

        if isinstance(node, list):
            return [_resolve(item, resolving) for item in node]

        return node

    return _resolve(schema)


class Blueprint(Generic[T]):
    """Base class for all Blueprint templates.

    Blueprints are reusable task templates that render into a single Airflow task
    or a TaskGroup. DAGs are composed from blueprint instances via YAML.

    Each blueprint defines a Pydantic config model and a render() method that
    produces tasks. The builder injects `step_id` before calling render().

    Example:
        class ExtractConfig(BaseModel):
            source_table: str
            batch_size: int = 1000

        class Extract(Blueprint[ExtractConfig]):
            def render(self, config: ExtractConfig) -> TaskGroup:
                with TaskGroup(group_id=self.step_id) as group:
                    BashOperator(task_id="extract", bash_command="...")
                return group

    Versioning:
        Version 1 uses a clean class name. Later versions add a V{N} suffix:

        class Extract(Blueprint[ExtractConfig]):       # v1
            ...

        class ExtractV2(Blueprint[ExtractV2Config]):   # v2
            ...

        The registry auto-detects the version from the class name.
    """

    _config_type: type[BaseModel]
    step_id: str
    name: str | None = None
    version: int | None = None
    supports_params: bool = False

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Extract config type from Generic[T] parameter when subclass is defined."""
        super().__init_subclass__(**kwargs)

        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            if hasattr(base, "__args__") and base.__args__:
                config_type = base.__args__[0]
                if isinstance(config_type, type) and issubclass(config_type, BaseModel):
                    cls._config_type = config_type
                    cls._validate_yaml_compatible_fields()
                    break

    def render(self, config: T) -> TaskOrGroup:
        """Render task(s) from the validated configuration.

        Must be implemented by Blueprint subclasses. Return either:
        - A single BaseOperator (for single-task blueprints)
        - A TaskGroup (for multi-task blueprints)

        Use self.step_id for the task_id or group_id.

        Args:
            config: The validated configuration model instance

        Returns:
            A single task or a TaskGroup
        """
        msg = f"{self.__class__.__name__} must implement the render() method"
        raise NotImplementedError(msg)

    def param(self, field: str) -> str:
        """Return a Jinja2 template string for a runtime-overridable config field.

        Use in operator template fields (e.g. ``bash_command``, ``configuration``)
        so Airflow renders the value at execution time from DAG params.

        Args:
            field: Name of a field on this blueprint's config model.

        Returns:
            A Jinja2 template string like ``{{ params.step__field }}``.

        Raises:
            ValueError: If the field does not exist on the config model.
        """
        config_type = self.get_config_type()
        if field not in config_type.model_fields:
            msg = (
                f"Unknown field '{field}' for {config_type.__name__}. "
                f"Available: {sorted(config_type.model_fields)}"
            )
            raise ValueError(msg)
        return "{{ params." + self.step_id + "__" + field + " }}"

    def resolve_config(self, config: T, context: dict[str, Any]) -> T:
        """Merge runtime params into config, returning a new validated instance.

        Use inside ``@task`` or ``PythonOperator`` callables to get config with
        runtime parameter overrides applied. The merged dict is re-validated
        through Pydantic, so invalid overrides raise ``ValidationError``.

        Args:
            config: The original validated config from render().
            context: The Airflow task execution context (``**context``).

        Returns:
            A new config instance with param overrides applied.
        """
        config_dict = config.model_dump()
        params = context.get("params", {})
        prefix = self.step_id + "__"
        for key, value in params.items():
            if key.startswith(prefix):
                field_name = key[len(prefix) :]
                if field_name in config_dict:
                    config_dict[field_name] = value
        return type(config).model_validate(config_dict)

    @classmethod
    def _validate_yaml_compatible_fields(cls) -> None:
        """Validate that all config fields use YAML-compatible types."""
        config_type = cls._config_type
        errors: list[str] = []

        for field_name, field_info in config_type.model_fields.items():
            error = _is_yaml_compatible(field_info.annotation)
            if error:
                errors.append(f"  {field_name}: {error}")

        if errors:
            fields_str = "\n".join(errors)
            msg = (
                f"{cls.__name__} config model {config_type.__name__} has "
                f"non-YAML-compatible fields:\n{fields_str}"
            )
            raise TypeError(msg)

    @classmethod
    def get_config_type(cls) -> type[BaseModel]:
        """Get the configuration type for this Blueprint."""
        if not hasattr(cls, "_config_type"):
            msg = (
                f"{cls.__name__} was not properly initialized. "
                "Make sure it inherits from Blueprint[ConfigType]"
            )
            raise RuntimeError(msg)
        return cls._config_type

    @classmethod
    def get_schema(cls) -> dict:
        """Get the JSON Schema for this Blueprint's configuration.

        Returns a flattened schema with all $ref/$defs resolved inline.
        """
        raw = cls.get_config_type().model_json_schema()
        return _resolve_refs(raw)

    @classmethod
    def get_source_code(cls) -> str:
        """Get the full source code of the file defining this Blueprint."""
        from pathlib import Path

        try:
            source_file = inspect.getfile(cls)
            return Path(source_file).read_text(encoding="utf-8")
        except (TypeError, OSError):
            return ""

    @classmethod
    def parse_name_and_version(cls) -> tuple[str, int]:
        """Extract blueprint name and version.

        Checks for explicit ``name`` and ``version`` class attributes first,
        then falls back to inferring from the class name:

        - 'Extract' -> ('extract', 1)
        - 'ExtractV2' -> ('extract', 2)
        - 'MultiSourceETLV3' -> ('multi_source_etl', 3)

        Explicit attributes can be set on the class body:

        - class MyExtractor(Blueprint[Cfg]):
              name = "extract"
              version = 3
        """
        explicit_name = cls.__dict__.get("name")
        explicit_version = cls.__dict__.get("version")

        if explicit_name is not None:
            if not isinstance(explicit_name, str) or not explicit_name:
                msg = f"{cls.__name__}: 'name' must be a non-empty string, got {explicit_name!r}"
                raise ValueError(msg)
            if not re.match(r"^[a-z][a-z0-9_]*$", explicit_name):
                msg = (
                    f"{cls.__name__}: 'name' must be snake_case "
                    f"(matching ^[a-z][a-z0-9_]*$), got {explicit_name!r}"
                )
                raise ValueError(msg)

        if explicit_version is not None and (
            not isinstance(explicit_version, int)
            or isinstance(explicit_version, bool)
            or explicit_version < 1
        ):
            msg = f"{cls.__name__}: 'version' must be an integer >= 1, got {explicit_version!r}"
            raise ValueError(msg)

        if explicit_name is not None and explicit_version is not None:
            return explicit_name, explicit_version

        class_name = cls.__name__
        version_match = re.match(r"^(.+?)V(\d+)$", class_name)
        if version_match:
            base_name = version_match.group(1)
            inferred_version = int(version_match.group(2))
        else:
            base_name = class_name
            inferred_version = 1

        snake_name = re.sub("([A-Z]+)([A-Z][a-z])", r"\1_\2", base_name)
        snake_name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", snake_name)
        inferred_name = snake_name.lower()

        return (
            explicit_name if explicit_name is not None else inferred_name,
            explicit_version if explicit_version is not None else inferred_version,
        )


class BlueprintDagArgs(Generic[T]):
    """Base class for DAG argument templates.

    Subclass this to define custom DAG constructor arguments with validated
    Pydantic config. At most one BlueprintDagArgs subclass may exist per project.

    The render() method receives the validated config and returns a dict of
    kwargs passed to the Airflow DAG constructor.

    Example:
        class MyDagArgsConfig(BaseModel):
            schedule: str | None = None
            owner: str = "data-team"
            retries: int = 2

        class MyDagArgs(BlueprintDagArgs[MyDagArgsConfig]):
            def render(self, config: MyDagArgsConfig) -> dict[str, Any]:
                return {
                    "schedule": config.schedule,
                    "default_args": {
                        "owner": config.owner,
                        "retries": config.retries,
                    },
                }
    """

    _config_type: type[BaseModel]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            if hasattr(base, "__args__") and base.__args__:
                config_type = base.__args__[0]
                if isinstance(config_type, type) and issubclass(config_type, BaseModel):
                    cls._config_type = config_type
                    cls._validate_yaml_compatible_fields()
                    break

    def render(self, config: T) -> dict[str, Any]:
        """Render DAG constructor kwargs from the validated configuration.

        Must be implemented by BlueprintDagArgs subclasses.

        Args:
            config: The validated configuration model instance

        Returns:
            Dict of kwargs to pass to the Airflow DAG constructor
        """
        msg = f"{self.__class__.__name__} must implement the render() method"
        raise NotImplementedError(msg)

    @classmethod
    def _validate_yaml_compatible_fields(cls) -> None:
        config_type = cls._config_type
        errors: list[str] = []

        for field_name, field_info in config_type.model_fields.items():
            error = _is_yaml_compatible(field_info.annotation)
            if error:
                errors.append(f"  {field_name}: {error}")

        if errors:
            fields_str = "\n".join(errors)
            msg = (
                f"{cls.__name__} config model {config_type.__name__} has "
                f"non-YAML-compatible fields:\n{fields_str}"
            )
            raise TypeError(msg)

    @classmethod
    def get_config_type(cls) -> type[BaseModel]:
        if not hasattr(cls, "_config_type"):
            msg = (
                f"{cls.__name__} was not properly initialized. "
                "Make sure it inherits from BlueprintDagArgs[ConfigType]"
            )
            raise RuntimeError(msg)
        return cls._config_type

    @classmethod
    def get_schema(cls) -> dict:
        raw = cls.get_config_type().model_json_schema()
        return _resolve_refs(raw)


class DefaultDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule: str | None = None
    description: str | None = None


class DefaultDagArgs(BlueprintDagArgs[DefaultDagArgsConfig]):
    """Built-in DAG arguments template providing schedule and description pass-through."""

    def render(self, config: DefaultDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description
        return kwargs
