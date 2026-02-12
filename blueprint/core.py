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

from pydantic import BaseModel

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
        """Extract blueprint name and version from the class name.

        - 'Extract' -> ('extract', 1)
        - 'ExtractV2' -> ('extract', 2)
        - 'MultiSourceETLV3' -> ('multi_source_etl', 3)
        """
        class_name = cls.__name__

        version_match = re.match(r"^(.+?)V(\d+)$", class_name)
        if version_match:
            base_name = version_match.group(1)
            version = int(version_match.group(2))
        else:
            base_name = class_name
            version = 1

        snake_name = re.sub("([A-Z]+)([A-Z][a-z])", r"\1_\2", base_name)
        snake_name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", snake_name)
        return snake_name.lower(), version
