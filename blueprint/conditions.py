"""Declarative conditions describing when a config field is applicable.

A field carries its condition in ``json_schema_extra``, so the published JSON
Schema and the check that runs at lint time read the same declaration.
"""

import operator as op
import re
import types
from typing import TYPE_CHECKING, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema
from pydantic_core import InitErrorDetails, PydanticCustomError
from pydantic_core import ValidationError as PydanticValidationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic_core import core_schema

APPLIES_WHEN_KEY = "x-blueprint-applies-when"
MANDATORY_KEY = "x-blueprint-mandatory"

Operator = Literal["==", "!=", ">", ">=", "<", "<=", "in", "not in", "matches", "set", "unset"]

OPERATORS: tuple[str, ...] = get_args(Operator)

_PRESENCE_OPERATORS = frozenset({"set", "unset"})
_ORDERING_OPERATORS = frozenset({">", ">=", "<", "<="})
_MEMBERSHIP_OPERATORS = frozenset({"in", "not in"})

_SCALAR_TYPES = (str, int, float, bool)
_NUMERIC_TYPES = (int, float)


class _Unset:
    def __repr__(self) -> str:
        return "<unset>"


_UNSET = _Unset()


def _matches(actual: object, expected: object) -> bool:
    return isinstance(actual, str) and re.search(str(expected), actual) is not None


_COMPARATORS: dict[str, Any] = {
    "==": op.eq,
    "!=": op.ne,
    ">": op.gt,
    ">=": op.ge,
    "<": op.lt,
    "<=": op.le,
    "in": lambda actual, expected: actual in expected,
    "not in": lambda actual, expected: actual not in expected,
    "matches": _matches,
}

_OPERATOR_SCHEMAS: dict[str, Any] = {
    "==": lambda value: {"const": value},
    "!=": lambda value: {"not": {"const": value}},
    ">": lambda value: {"exclusiveMinimum": value},
    ">=": lambda value: {"minimum": value},
    "<": lambda value: {"exclusiveMaximum": value},
    "<=": lambda value: {"maximum": value},
    "in": lambda value: {"enum": list(value)},
    "not in": lambda value: {"not": {"enum": list(value)}},
    "matches": lambda value: {"pattern": value},
}


class ConditionExpr:
    """Base class for the condition tree attached to a field."""

    def to_json(self) -> dict[str, Any]:
        """Serialize to the form stored in the JSON Schema."""
        raise NotImplementedError

    def describe(self) -> str:
        """Render the condition the way error messages spell it."""
        raise NotImplementedError

    def atoms(self) -> list["Condition"]:
        """Every comparison in the tree, for declaration-time checks."""
        raise NotImplementedError


class Condition(ConditionExpr):
    """A comparison of a sibling field against a literal value.

    Example:
        Condition("schedule_type", "==", "asset")
        Condition("region", "in", ["us", "eu"])
        Condition("archive_path", "set")
    """

    def __init__(self, field: str, operator: Operator, value: Any = _UNSET) -> None:
        self.field = field
        self.operator = operator
        self.value = value

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {"field": self.field, "op": self.operator}
        if not isinstance(self.value, _Unset):
            data["value"] = self.value
        return data

    def describe(self) -> str:
        if self.operator in _PRESENCE_OPERATORS:
            return f"{self.field} is {self.operator}"
        return f"{self.field} {self.operator} {self.value!r}"

    def atoms(self) -> list["Condition"]:
        return [self]


class _Group(ConditionExpr):
    _keyword: str
    _joiner: str

    def __init__(self, *conditions: Any) -> None:
        if len(conditions) == 1 and isinstance(conditions[0], list | tuple):
            conditions = tuple(conditions[0])
        self.conditions: list[ConditionExpr] = list(conditions)

    def to_json(self) -> dict[str, Any]:
        return {self._keyword: [c.to_json() for c in self.conditions]}

    def describe(self) -> str:
        return "(" + f" {self._joiner} ".join(c.describe() for c in self.conditions) + ")"

    def atoms(self) -> list[Condition]:
        return [atom for c in self.conditions for atom in c.atoms()]


class AllOf(_Group):
    """Holds when every nested condition holds."""

    _keyword = "allOf"
    _joiner = "and"


class AnyOf(_Group):
    """Holds when at least one nested condition holds."""

    _keyword = "anyOf"
    _joiner = "or"


class Not(ConditionExpr):
    """Holds when the nested condition does not."""

    def __init__(self, condition: ConditionExpr) -> None:
        self.condition = condition

    def to_json(self) -> dict[str, Any]:
        return {"not": self.condition.to_json()}

    def describe(self) -> str:
        return f"not {self.condition.describe()}"

    def atoms(self) -> list[Condition]:
        return self.condition.atoms()


def from_json(data: Any) -> ConditionExpr:
    """Rebuild a condition tree from its JSON Schema form.

    Raises:
        TypeError: If the condition is not a mapping.
        ValueError: If the mapping is not shaped like a condition.
    """
    if not isinstance(data, dict):
        msg = f"condition must be a mapping, got {type(data).__name__}"
        raise TypeError(msg)

    for keyword, group in (("allOf", AllOf), ("anyOf", AnyOf)):
        if keyword in data:
            items = data[keyword]
            if not isinstance(items, list) or not items:
                msg = f"'{keyword}' must hold at least one condition"
                raise ValueError(msg)
            return group([from_json(item) for item in items])

    if "not" in data:
        return Not(from_json(data["not"]))

    if "field" not in data or "op" not in data:
        msg = f"condition needs 'field' and 'op' keys, got {sorted(data)}"
        raise ValueError(msg)

    return Condition(data["field"], data["op"], data.get("value", _UNSET))


def field_condition(field_info: Any) -> tuple[ConditionExpr | None, bool]:
    """Read the condition and mandatory flag a field declares, if any."""
    extra = field_info.json_schema_extra
    if not isinstance(extra, dict) or APPLIES_WHEN_KEY not in extra:
        return None, False
    return from_json(extra[APPLIES_WHEN_KEY]), bool(extra.get(MANDATORY_KEY))


def evaluate(expr: ConditionExpr, values: "BaseModel | Mapping[str, Any]") -> bool:
    """Evaluate a condition against a validated model or a mapping of field values."""
    if isinstance(expr, AllOf):
        return all(evaluate(c, values) for c in expr.conditions)
    if isinstance(expr, AnyOf):
        return any(evaluate(c, values) for c in expr.conditions)
    if isinstance(expr, Not):
        return not evaluate(expr.condition, values)

    atom: Condition = expr  # type: ignore[assignment]
    actual = _lookup(values, atom.field)
    if atom.operator in _PRESENCE_OPERATORS:
        return (actual is not None) is (atom.operator == "set")
    return _compare(actual, atom.operator, atom.value)


def applies(condition_json: Any, values: "Mapping[str, Any]") -> bool:
    """Whether a condition published in a JSON Schema holds for a mapping of values."""
    return evaluate(from_json(condition_json), values)


def _lookup(values: "BaseModel | Mapping[str, Any]", field: str) -> object:
    """Read a field, treating an explicit null the same as an absent one."""
    if isinstance(values, BaseModel):
        return getattr(values, field, None)
    return values.get(field)


def _compare(actual: object, operator: str, expected: object) -> bool:
    """Compare two values, treating an incomparable pair as a failed condition."""
    try:
        return bool(_COMPARATORS[operator](actual, expected))
    except TypeError:
        return False


def check(instance: BaseModel) -> None:
    """Enforce every ``applies_when`` declaration reachable from an instance.

    Args:
        instance: A validated config model.

    Raises:
        pydantic.ValidationError: With one error per violation, located on the
            offending field so existing config error formatting picks it up.
    """
    line_errors = _collect(instance, ())
    if line_errors:
        raise PydanticValidationError.from_exception_data(
            title=type(instance).__name__, line_errors=line_errors
        )


def _collect(instance: BaseModel, loc: tuple[str | int, ...]) -> list[InitErrorDetails]:
    line_errors: list[InitErrorDetails] = []

    for name, field_info in type(instance).model_fields.items():
        value = getattr(instance, name, None)
        expr, mandatory = field_condition(field_info)

        if expr is not None:
            applicable = evaluate(expr, instance)
            if not applicable and value is not None:
                line_errors.append(
                    _error((*loc, name), value, f"only applicable when {expr.describe()}")
                )
            elif applicable and mandatory and value is None:
                line_errors.append(_error((*loc, name), value, f"required when {expr.describe()}"))

        line_errors.extend(_collect_nested(value, (*loc, name)))

    return line_errors


def _collect_nested(value: object, loc: tuple[str | int, ...]) -> list[InitErrorDetails]:
    if isinstance(value, BaseModel):
        return _collect(value, loc)
    if isinstance(value, list):
        return [
            error
            for index, item in enumerate(value)
            if isinstance(item, BaseModel)
            for error in _collect(item, (*loc, index))
        ]
    if isinstance(value, dict):
        return [
            error
            for key, item in value.items()
            if isinstance(item, BaseModel)
            for error in _collect(item, (*loc, key))
        ]
    return []


def _error(loc: tuple[str | int, ...], value: object, message: str) -> InitErrorDetails:
    return InitErrorDetails(
        type=PydanticCustomError("blueprint_applies_when", "{detail}", {"detail": message}),
        loc=loc,
        input=value,
    )


def declaration_errors(model: type[BaseModel]) -> list[str]:
    """Return one message per invalid declaration on a model or the models it nests."""
    return _model_errors(model, prefix="", seen=set())


def _model_errors(model: type[BaseModel], prefix: str, seen: set[int]) -> list[str]:
    if id(model) in seen:
        return []
    seen.add(id(model))

    errors: list[str] = []
    for name, field_info in model.model_fields.items():
        errors.extend(
            f"{prefix}{name}: {error}" for error in _field_errors(model, name, field_info)
        )
        for nested in _nested_models(field_info.annotation):
            errors.extend(_model_errors(nested, f"{prefix}{name}.", seen))
    return errors


def _field_errors(model: type[BaseModel], name: str, field_info: Any) -> list[str]:
    extra = field_info.json_schema_extra
    if not isinstance(extra, dict):
        return []

    if APPLIES_WHEN_KEY not in extra:
        if extra.get(MANDATORY_KEY):
            return ["'mandatory' has no meaning without 'applies_when'"]
        return []

    try:
        expr = from_json(extra[APPLIES_WHEN_KEY])
    except (TypeError, ValueError) as e:
        return [str(e)]

    errors: list[str] = []
    if field_info.is_required() or field_info.get_default(call_default_factory=True) is not None:
        errors.append(
            "a field with 'applies_when' must be optional and default to None "
            "(declare it as 'X | None = None'), so an inapplicable field is absent"
        )

    for atom in expr.atoms():
        errors.extend(f"{atom.describe()}: {error}" for error in _atom_errors(model, name, atom))

    return errors


def _atom_errors(model: type[BaseModel], owner: str, atom: Condition) -> list[str]:  # noqa: PLR0911
    if atom.operator not in OPERATORS:
        return [f"unknown operator '{atom.operator}', expected one of {list(OPERATORS)}"]

    if atom.field == owner:
        return ["a condition cannot reference the field it is declared on"]

    field_info = model.model_fields.get(atom.field)
    if field_info is None:
        return [
            f"unknown field '{atom.field}', expected one of {sorted(model.model_fields)}",
        ]

    if atom.operator in _PRESENCE_OPERATORS:
        if not isinstance(atom.value, _Unset):
            return [f"operator '{atom.operator}' takes no value"]
        return []

    if isinstance(atom.value, _Unset):
        return [f"operator '{atom.operator}' needs a value"]
    if atom.value is None:
        return ["compare against a value, or use the 'set' / 'unset' operators"]

    base = _unwrap_optional(field_info.annotation)
    if not _is_scalar(base):
        return [
            f"'{atom.field}' is not a scalar field, so only 'set' and 'unset' can be used with it"
        ]

    return _value_errors(atom, base)


def _value_errors(atom: Condition, base: Any) -> list[str]:
    if atom.operator in _ORDERING_OPERATORS:
        return _ordering_errors(atom, base)
    if atom.operator in _MEMBERSHIP_OPERATORS:
        return _membership_errors(atom, base)
    if atom.operator == "matches":
        return _pattern_errors(atom, base)
    if not _value_matches(atom.value, base):
        return [f"{atom.value!r} is not a valid {_type_name(base)} value"]
    return []


def _ordering_errors(atom: Condition, base: Any) -> list[str]:
    if base not in _NUMERIC_TYPES:
        return [f"'{atom.operator}' needs a numeric field, got {_type_name(base)}"]
    if not _is_numeric(atom.value):
        return [f"'{atom.operator}' needs a numeric value, got {atom.value!r}"]
    return []


def _membership_errors(atom: Condition, base: Any) -> list[str]:
    if not isinstance(atom.value, list | tuple) or not atom.value:
        return [f"'{atom.operator}' needs a non-empty list of values, got {atom.value!r}"]
    return [
        f"{member!r} is not a valid {_type_name(base)} value"
        for member in atom.value
        if not _value_matches(member, base)
    ]


def _pattern_errors(atom: Condition, base: Any) -> list[str]:
    if base is not str:
        return [f"'matches' needs a string field, got {_type_name(base)}"]
    if not isinstance(atom.value, str):
        return [f"'matches' needs a string pattern, got {atom.value!r}"]
    try:
        re.compile(atom.value)
    except re.error as e:
        return [f"invalid regular expression: {e}"]
    return []


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        return args[0] if len(args) == 1 else None
    return annotation


def _is_scalar(base: Any) -> bool:
    return base in _SCALAR_TYPES or get_origin(base) is Literal


def _is_numeric(value: object) -> bool:
    return isinstance(value, _NUMERIC_TYPES) and not isinstance(value, bool)


def _value_matches(value: object, base: Any) -> bool:
    if get_origin(base) is Literal:
        return value in get_args(base)
    if base is bool:
        return isinstance(value, bool)
    if base is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if base is float:
        return _is_numeric(value)
    return isinstance(value, str)


def _type_name(base: Any) -> str:
    if get_origin(base) is Literal:
        return "one of " + ", ".join(repr(arg) for arg in get_args(base))
    return getattr(base, "__name__", str(base))


def _nested_models(annotation: Any) -> list[type[BaseModel]]:
    base = _unwrap_optional(annotation)
    origin = get_origin(base)
    args = get_args(base)
    if origin in (list, dict) and args:
        return _nested_models(args[-1])
    if isinstance(base, type) and issubclass(base, BaseModel):
        return [base]
    return []


def object_clauses(model: type[BaseModel]) -> list[dict[str, Any]]:
    """Compile a model's conditions into standard ``if``/``then``/``else`` clauses.

    ``then`` carries requiredness and ``else`` carries applicability, so a generic
    JSON Schema validator enforces what ``check()`` enforces in Python.
    """
    clauses: list[dict[str, Any]] = []

    for name, field_info in model.model_fields.items():
        try:
            expr, mandatory = field_condition(field_info)
        except (TypeError, ValueError):
            continue
        if expr is None:
            continue

        clause: dict[str, Any] = {"if": _condition_schema(expr, model)}
        if mandatory:
            clause["then"] = {"required": [name]}
        clause["else"] = {"not": {"required": [name]}}
        clauses.append(clause)

    return clauses


def _condition_schema(expr: ConditionExpr, model: type[BaseModel]) -> dict[str, Any]:
    if isinstance(expr, _Group):
        branches = [_condition_schema(c, model) for c in expr.conditions]
        flattened = [
            nested
            for branch in branches
            for nested in (branch[expr._keyword] if set(branch) == {expr._keyword} else [branch])
        ]
        if len(flattened) == 1:
            return flattened[0]
        return {expr._keyword: flattened}

    if isinstance(expr, Not):
        return {"not": _condition_schema(expr.condition, model)}

    return _atom_schema(expr, model)  # type: ignore[arg-type]


def _atom_schema(atom: Condition, model: type[BaseModel]) -> dict[str, Any]:
    """Compile one comparison, folding in the case of an absent field with a default.

    JSON Schema never applies defaults, so a field left out of the YAML would fail an
    ``if`` that its own default satisfies. Folding happens per atom: at the root it
    would invert under ``not`` and mix wrongly under ``anyOf``.
    """
    absent = {"not": {"required": [atom.field]}}
    if atom.operator in _PRESENCE_OPERATORS:
        return {"required": [atom.field]} if atom.operator == "set" else absent

    present = {
        "properties": {atom.field: _OPERATOR_SCHEMAS[atom.operator](atom.value)},
        "required": [atom.field],
    }

    field_info = model.model_fields.get(atom.field)
    if field_info is None or field_info.is_required():
        return present

    default = field_info.get_default(call_default_factory=True)
    if default is not None and _compare(default, atom.operator, atom.value):
        return {"anyOf": [present, absent]}
    return present


class ConditionSchemaGenerator(GenerateJsonSchema):
    """Emits each model's conditions as ``if``/``then``/``else`` on its own object schema."""

    def model_schema(self, schema: "core_schema.ModelSchema") -> dict[str, Any]:
        json_schema = super().model_schema(schema)
        model = schema["cls"]
        if isinstance(model, type) and issubclass(model, BaseModel):
            clauses = object_clauses(model)
            if clauses:
                json_schema.setdefault("allOf", []).extend(clauses)
        return json_schema
