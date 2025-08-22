"""Blueprint - Reusable, validated Airflow DAG templates."""

__version__ = "0.0.1a2"

from .core import Blueprint
from .errors import (
    BlueprintError,
    BlueprintNotFoundError,
    ConfigurationError,
    DuplicateBlueprintError,
    DuplicateDAGIdError,
    YAMLParseError,
)
from .errors import (
    ValidationError as BlueprintValidationError,
)
from .loaders import (
    discover_blueprints,
    from_yaml,
    get_blueprint_info,
    load_blueprint,
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
from .registry import registry
from .template_loader import (
    auto_load_yaml_dags,
    discover_yaml_dags,
    load_template,
    setup_template_path,
)

__all__ = [
    "Any",
    "BaseModel",
    "Blueprint",
    "BlueprintError",
    "BlueprintNotFoundError",
    "BlueprintValidationError",
    "ConfigDict",
    "ConfigurationError",
    "Dict",
    "DuplicateBlueprintError",
    "DuplicateDAGIdError",
    "Field",
    "List",
    "Optional",
    "Union",
    "ValidationError",
    "YAMLParseError",
    "auto_load_yaml_dags",
    "discover_blueprints",
    "discover_yaml_dags",
    "field_validator",
    "from_yaml",
    "get_blueprint_info",
    "load_blueprint",
    "load_template",
    "model_validator",
    "registry",
    "setup_template_path",
]
