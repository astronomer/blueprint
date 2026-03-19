"""Blueprint - Reusable task templates composed into Airflow DAGs via YAML."""

__version__ = "0.1.1"

from .builder import Builder, DAGConfig, StepConfig, build_all
from .core import Blueprint, BlueprintDagArgs, DefaultDagArgs, TaskOrGroup
from .errors import (
    BlueprintError,
    BlueprintNotFoundError,
    ConfigurationError,
    CyclicDependencyError,
    DuplicateBlueprintError,
    DuplicateDAGIdError,
    InvalidDependencyError,
    InvalidVersionError,
    MultipleDagArgsError,
    NonContiguousVersionError,
    YAMLParseError,
)
from .errors import (
    ValidationError as BlueprintValidationError,
)
from .loaders import (
    discover_blueprints,
    get_blueprint_info,
    load_blueprint,
    validate_yaml,
)
from .models import (
    Any,
    BaseModel,
    ConfigDict,
    Dict,
    Field,
    List,
    Optional,
    Union,
    ValidationError,
    field_validator,
    model_validator,
)
from .registry import BlueprintRegistry, registry

__all__ = [
    "Any",
    "BaseModel",
    "Blueprint",
    "BlueprintDagArgs",
    "BlueprintError",
    "BlueprintNotFoundError",
    "BlueprintRegistry",
    "BlueprintValidationError",
    "Builder",
    "ConfigDict",
    "ConfigurationError",
    "CyclicDependencyError",
    "DAGConfig",
    "DefaultDagArgs",
    "Dict",
    "DuplicateBlueprintError",
    "DuplicateDAGIdError",
    "Field",
    "InvalidDependencyError",
    "InvalidVersionError",
    "List",
    "MultipleDagArgsError",
    "NonContiguousVersionError",
    "Optional",
    "StepConfig",
    "TaskOrGroup",
    "Union",
    "ValidationError",
    "YAMLParseError",
    "build_all",
    "discover_blueprints",
    "field_validator",
    "get_blueprint_info",
    "load_blueprint",
    "model_validator",
    "registry",
    "validate_yaml",
]
