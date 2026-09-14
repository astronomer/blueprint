"""Pydantic model utilities and re-exports for Blueprint."""

# Re-export commonly used Pydantic components
# Re-export type annotations commonly used with Pydantic
from typing import Any, Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic import Field as PydanticField
from pydantic_core import PydanticUndefined

from blueprint.conditions import APPLIES_WHEN_KEY, MANDATORY_KEY, ConditionExpr

__all__ = [
    "Any",
    # Pydantic exports
    "BaseModel",
    "ConfigDict",
    "Dict",
    "Field",
    "List",
    # Type exports
    "Optional",
    "Union",
    "ValidationError",
    "field_validator",
    "model_validator",
]


def Field(  # noqa: N802
    default: Any = PydanticUndefined,
    *,
    applies_when: ConditionExpr | None = None,
    mandatory: bool = False,
    **kwargs: Any,
) -> Any:
    """Declare a config field, optionally conditional on other fields.

    Accepts everything ``pydantic.Field`` accepts, plus:

    Args:
        default: The field's default, as for ``pydantic.Field``.
        applies_when: Condition under which this field is in play. When it does
            not hold, setting the field is an error. The condition is published
            in the blueprint's JSON Schema, so editors can hide the field.
        mandatory: Whether the field must be set whenever it does apply.
        **kwargs: Passed through to ``pydantic.Field``.

    Example:
        schedule_type: Literal["time", "asset"] = "time"
        cron: str | None = Field(
            None,
            applies_when=Condition("schedule_type", "==", "time"),
            mandatory=True,
        )
    """
    if mandatory and applies_when is None:
        msg = "'mandatory' has no meaning without 'applies_when'"
        raise ValueError(msg)

    if applies_when is not None:
        if not isinstance(applies_when, ConditionExpr):
            msg = (
                f"'applies_when' must be a Condition, AllOf, AnyOf or Not, "
                f"got {type(applies_when).__name__}"
            )
            raise TypeError(msg)

        declared = kwargs.get("json_schema_extra")
        if declared is not None and not isinstance(declared, dict):
            msg = "'json_schema_extra' must be a dict to combine it with 'applies_when'"
            raise TypeError(msg)

        extra = dict(declared or {})
        extra[APPLIES_WHEN_KEY] = applies_when.to_json()
        if mandatory:
            extra[MANDATORY_KEY] = True
        kwargs["json_schema_extra"] = extra

    return PydanticField(default, **kwargs)
