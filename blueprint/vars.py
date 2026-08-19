"""Declarative variable resolution for DAG YAML definitions.

Variables are declared in ``blueprint.vars.yaml`` files alongside the DAGs they
serve, and in a ``vars:`` block inside a ``.dag.yaml``. They are referenced as
``${name}`` and resolved after YAML parsing, so a reference occupying an entire
value keeps its underlying type.

Files closer to a DAG override those further away, and a DAG's own ``vars:``
block overrides every file::

    vars:
      landing_dataset: raw_events
      warehouse_db: analytics

Profiles are optional. Declaring them, once in the outermost vars file, lets a
variable carry a different value per environment; a map value is only ever a set
of per-profile values, never literal data::

    profiles: [prod, dev]

    vars:
      warehouse_db:
        prod: analytics
        dev: sandbox
"""

import re
from pathlib import Path
from typing import Any

import yaml

from blueprint.errors import (
    CompositionDepthError,
    CyclicVariableError,
    IncompleteVariableError,
    InvalidVariableNameError,
    InvalidVariableValueError,
    ProfileError,
    UndefinedVariableError,
)

VARS_FILENAME = "blueprint.vars.yaml"

# ``${name}`` where name has no period -- periods are reserved so that dotted
# namespaces (``${env.FOO}``) can be introduced later without ambiguity.
REFERENCE_RE = re.compile(r"\$\{([^}]*)\}")

# ``$${`` escapes a reference, so ``$${HOME}`` survives as ``${HOME}``. The
# lookahead keeps a bare ``$$`` (the shell PID, awk fields) untouched.
ESCAPE_OR_REFERENCE_RE = re.compile(r"\$\$(?=\{)|\$\{([^}]*)\}")

VALID_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")

MAX_COMPOSITION_DEPTH = 50


class ResolvedVars:
    """Variables resolved for a single DAG under a single profile.

    Resolution is lazy: ``values`` holds only what the DAG actually referenced.
    ``available`` holds everything in scope, with profile-varying entries still
    keyed by profile.
    """

    def __init__(
        self,
        values: dict[str, Any],
        profile: str | None,
        sources: dict[str, Path | str],
        available: dict[str, Any] | None = None,
        referenced: set[str] | None = None,
        resolver: Any = None,
    ):
        self.values = values
        self.profile = profile
        self.sources = sources
        self.available = available if available is not None else dict(values)
        self.referenced = referenced or set()
        self._resolver = resolver

    def resolve_name(self, name: str) -> Any:
        """Resolve a single variable by name, raising if it cannot be resolved."""
        if self._resolver is None:
            return self.values[name]
        return self._resolver(name)

    def __repr__(self) -> str:
        return f"ResolvedVars(profile={self.profile!r}, values={self.values!r})"


def discover_vars_files(start: Path, stop: Path | None = None) -> list[Path]:
    """Find ``blueprint.vars.yaml`` files from the project root down to ``start``.

    ``.airflowignore`` does not apply: a vars file only ever serves DAGs in its
    own directory or below, so an ignored directory's vars are already unused by
    everything Airflow loads.

    Args:
        start: Directory containing the DAG file.
        stop: Optional outermost directory to search; defaults to the filesystem root.

    Returns:
        Paths ordered outermost first, so nearer files override further ones.
    """
    start = start.resolve()
    boundary = stop.resolve() if stop is not None else None

    found: list[Path] = []

    current = start
    while True:
        candidate = current / VARS_FILENAME
        if candidate.is_file():
            found.append(candidate)
        if boundary is not None and current == boundary:
            break
        if current.parent == current or (boundary is not None and boundary not in current.parents):
            break
        current = current.parent

    return list(reversed(found))


def _load_vars_file(path: Path) -> tuple[list[str] | None, dict[str, Any]]:
    """Parse a vars file into its declared profiles and variable block."""
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        msg = f"Failed to parse {path.name}: {e}"
        raise ProfileError(msg, path) from e

    if not isinstance(raw, dict):
        msg = f"{path.name} must contain a mapping"
        raise ProfileError(msg, path)

    profiles = raw.get("profiles")
    if profiles is not None and (
        not isinstance(profiles, list) or any(not isinstance(p, str) for p in profiles)
    ):
        msg = "'profiles' must be a list of strings"
        raise ProfileError(msg, path)

    declared = raw.get("vars") or {}
    if not isinstance(declared, dict):
        msg = "'vars' must be a mapping"
        raise ProfileError(msg, path)

    return profiles, declared


def _validate_names(declared: dict[str, Any], source: Path | str) -> None:
    """Reject variable names that would collide with dotted namespaces."""
    for name in declared:
        if not isinstance(name, str) or not VALID_NAME_RE.match(name):
            raise InvalidVariableNameError(str(name), source)


SCALAR_TYPES = (str, int, float, bool, type(None))


def _check_plain_value(name: str, value: Any, source: Path | str) -> None:
    """Reject anything that is not a scalar or a list of scalars, at any depth."""
    if isinstance(value, SCALAR_TYPES):
        return
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                msg = "maps are not allowed inside list values"
                raise InvalidVariableValueError(name, msg, source)
            _check_plain_value(name, item, source)
        return

    msg = f"{type(value).__name__} values are not supported"
    raise InvalidVariableValueError(name, msg, source)


def _check_value(name: str, value: Any, profiles: list[str], source: Path | str) -> bool:
    """Validate a declared value, returning True when it is a profile map.

    A mapping is only ever a per-profile value. Anything else must be a scalar
    or a list of scalars, so ``${a.b}`` never has a second possible meaning.
    """
    if not isinstance(value, dict):
        _check_plain_value(name, value, source)
        return False

    if not profiles:
        msg = "maps are only allowed for per-profile values, but no profiles are declared"
        raise InvalidVariableValueError(
            name,
            msg,
            source,
            suggestions=[
                f"Declare `profiles: [...]` in {VARS_FILENAME}",
                "Or use a scalar or list value",
            ],
        )

    unknown = [k for k in value if not isinstance(k, str) or k not in profiles]
    if unknown:
        listed = ", ".join(repr(k) for k in unknown)
        msg = f"map keys must all be declared profiles; unknown: {listed}"
        raise InvalidVariableValueError(
            name,
            msg,
            source,
            suggestions=[
                f"Declared profiles: {', '.join(profiles)}",
                "Maps are reserved for per-profile values and cannot hold literal data",
            ],
        )

    for profile_value in value.values():
        _check_plain_value(name, profile_value, source)

    return True


def _is_profile_map(value: Any, profiles: list[str]) -> bool:
    """A mapping is profile-keyed when every key is a declared profile."""
    if not isinstance(value, dict) or not value or not profiles:
        return False
    return all(isinstance(k, str) and k in profiles for k in value)


def _merge_layer(
    merged: dict[str, Any],
    sources: dict[str, Path | str],
    profile_sources: dict[str, dict[str, Path | str]],
    declared: dict[str, Any],
    profiles: list[str],
    source: Path | str,
) -> None:
    """Merge one scope's variables over the accumulated result."""
    _validate_names(declared, source)

    for name, value in declared.items():
        _check_value(name, value, profiles, source)
        if _is_profile_map(value, profiles):
            if _is_profile_map(merged.get(name), profiles):
                combined = dict(merged[name])
                combined.update(value)
                merged[name] = combined
            else:
                merged[name] = value
                profile_sources.pop(name, None)
            # Track per profile so a partial override reports the right origin.
            per_profile = profile_sources.setdefault(name, {})
            for key in value:
                per_profile[key] = source
        else:
            merged[name] = value
            profile_sources.pop(name, None)
        sources[name] = source


def _make_resolver(
    merged: dict[str, Any],
    profiles: list[str],
    profile: str | None,
    sources: dict[str, Path | str],
    profile_sources: dict[str, dict[str, Path | str]],
    referenced: set[str],
) -> tuple[Any, dict[str, Any]]:
    """Build a lazy variable resolver.

    Values are resolved only when something references them, so a DAG that never
    uses a profile-varying variable does not need a profile selected.
    """
    cache: dict[str, Any] = {}
    resolving: list[str] = []

    def value_of(name: str, source: Any = None) -> Any:
        if name in cache:
            return cache[name]

        if "." in name:
            raise UndefinedVariableError(
                name,
                sorted(merged),
                source,
                detail="dotted references are reserved for future namespaces",
            )
        if name not in merged:
            raise UndefinedVariableError(name, sorted(merged), source)

        if name in resolving:
            cycle = [*resolving[resolving.index(name) :], name]
            raise CyclicVariableError(cycle, sources.get(name))
        if len(resolving) > MAX_COMPOSITION_DEPTH:
            raise CompositionDepthError(
                [*resolving, name], MAX_COMPOSITION_DEPTH, sources.get(name)
            )

        value = merged[name]

        if _is_profile_map(value, profiles):
            if profile is None:
                msg = (
                    f"Variable '{name}' varies by profile but no profile was selected. "
                    f"Declared profiles: {', '.join(profiles)}"
                )
                raise ProfileError(msg, sources.get(name))
            if profile not in value:
                raise IncompleteVariableError(name, profile, sorted(value), sources.get(name))
            origin = profile_sources.get(name, {}).get(profile)
            if origin is not None:
                sources[name] = origin
            value = value[profile]

        if _contains_reference(value):
            resolving.append(name)
            try:
                value = _interpolate(value, value_of, sources.get(name), referenced)
            finally:
                resolving.pop()

        cache[name] = value
        return value

    return value_of, cache


def _contains_reference(value: Any) -> bool:
    """True when a value, or anything inside a list value, holds a reference."""
    if isinstance(value, str):
        return "${" in value
    if isinstance(value, list):
        return any(_contains_reference(item) for item in value)
    return False


def _substitute(text: str, value_of: Any, source: Any, referenced: set[str] | None) -> Any:
    """Substitute ``${...}`` references, preserving type for a whole-value match."""
    whole = REFERENCE_RE.fullmatch(text)

    def lookup(name: str) -> Any:
        if referenced is not None:
            referenced.add(name)
        return value_of(name, source)

    if whole is not None:
        return lookup(whole.group(1).strip())

    def replace(match: re.Match) -> str:
        if match.group(0) == "$$":
            return "$"
        return str(lookup(match.group(1).strip()))

    return ESCAPE_OR_REFERENCE_RE.sub(replace, text)


def _interpolate(node: Any, value_of: Any, source: Any, referenced: set[str]) -> Any:
    """Walk a parsed config tree substituting variable references."""
    if isinstance(node, str):
        return _substitute(node, value_of, source, referenced)
    if isinstance(node, dict):
        return {k: _interpolate(v, value_of, source, referenced) for k, v in node.items()}
    if isinstance(node, list):
        return [_interpolate(v, value_of, source, referenced) for v in node]
    return node


def resolve(
    config: dict[str, Any],
    path: Path,
    profile: str | None = None,
    search_root: Path | None = None,
) -> tuple[dict[str, Any], ResolvedVars]:
    """Resolve variables for one DAG config and substitute them into it.

    Args:
        config: The parsed .dag.yaml contents.
        path: Path to the .dag.yaml, used to locate vars files.
        profile: Active profile name. Only required if a referenced variable varies.
        search_root: Outermost directory searched for vars files; defaults to the
            YAML file's own directory so the walk is never unbounded.

    Returns:
        Tuple of (config with ``vars`` removed and references substituted,
        the resolved variables).
    """
    merged: dict[str, Any] = {}
    sources: dict[str, Path | str] = {}
    profile_sources: dict[str, dict[str, Path | str]] = {}
    profiles: list[str] = []

    profiles_declared_in: Path | None = None
    for vars_file in discover_vars_files(path.parent, search_root or path.parent):
        declared, declared_vars = _load_vars_file(vars_file)
        if declared is not None:
            if profiles_declared_in is not None:
                msg = (
                    f"'profiles' is already declared in {profiles_declared_in}; "
                    "declare it once, in the outermost vars file"
                )
                raise ProfileError(msg, vars_file)
            profiles = declared
            profiles_declared_in = vars_file
        _merge_layer(merged, sources, profile_sources, declared_vars, profiles, vars_file)

    dag_vars = config.get("vars") or {}
    if not isinstance(dag_vars, dict):
        msg = "'vars' must be a mapping"
        raise ProfileError(msg, path)
    _merge_layer(merged, sources, profile_sources, dag_vars, profiles, path)

    if profile is not None and profiles and profile not in profiles:
        msg = f"Unknown profile '{profile}'. Declared profiles: {', '.join(profiles)}"
        raise ProfileError(msg, path)

    remaining = {k: v for k, v in config.items() if k != "vars"}

    # ``${...}`` is always a variable reference, whether or not this project
    # declares any variables. Anything meant literally is written ``$${...}``,
    # so the meaning of a document never depends on state outside it.
    referenced: set[str] = set()
    value_of, cache = _make_resolver(
        merged, profiles, profile, sources, profile_sources, referenced
    )
    substituted = _interpolate(remaining, value_of, path, referenced)

    return substituted, ResolvedVars(
        cache,
        profile,
        sources,
        available=merged,
        referenced=referenced,
        resolver=value_of,
    )


def collect(
    config: dict[str, Any],
    path: Path,
    profile: str | None = None,
    search_root: Path | None = None,
) -> ResolvedVars:
    """Collect the variables in scope without substituting them into a config.

    Unlike :func:`resolve` this never fails on a profile-varying variable, so
    introspection can show what is in scope even with no profile selected.

    Args:
        config: The parsed .dag.yaml contents.
        path: Path to the .dag.yaml, used to locate vars files.
        profile: Active profile name, or None.
        search_root: Outermost directory searched for vars files; defaults to the
            YAML file's own directory so the walk is never unbounded.

    Returns:
        The variables in scope, resolvable one at a time via ``resolve_name``.
    """
    merged: dict[str, Any] = {}
    sources: dict[str, Path | str] = {}
    profile_sources: dict[str, dict[str, Path | str]] = {}
    profiles: list[str] = []

    for vars_file in discover_vars_files(path.parent, search_root or path.parent):
        declared, declared_vars = _load_vars_file(vars_file)
        if declared is not None:
            profiles = declared
        _merge_layer(merged, sources, profile_sources, declared_vars, profiles, vars_file)

    dag_vars = config.get("vars") or {}
    if isinstance(dag_vars, dict):
        _merge_layer(merged, sources, profile_sources, dag_vars, profiles, path)

    value_of, cache = _make_resolver(merged, profiles, profile, sources, profile_sources, set())
    return ResolvedVars(cache, profile, sources, available=merged, resolver=value_of)


def declared_profiles(path: Path, search_root: Path | None = None) -> list[str]:
    """Return the profiles declared for the DAG at ``path``.

    Does not raise on duplicate ``profiles:`` declarations, unlike ``resolve``:
    the innermost declaration wins here. Call ``resolve`` to validate.
    """
    profiles: list[str] = []
    for vars_file in discover_vars_files(path.parent, search_root or path.parent):
        declared, _ = _load_vars_file(vars_file)
        if declared is not None:
            profiles = declared
    return profiles


def unused_variables(resolved: ResolvedVars) -> list[str]:
    """Return variables in scope that this DAG never references."""
    return sorted(set(resolved.available) - resolved.referenced)
