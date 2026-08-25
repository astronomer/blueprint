"""Blueprint error handling with rich context and suggestions."""

import difflib
from pathlib import Path
from typing import Any

import yaml

from blueprint.utils import display_path

# Constants
MAX_SUGGESTION_VALUES = 10
VARS_FILENAME_HINT = "blueprint.vars.yaml"


class BlueprintError(Exception):
    """Base exception for all blueprint errors."""


class ConfigurationError(BlueprintError):
    """Configuration-related errors with rich context."""

    def __init__(
        self,
        message: str,
        file_path: Path | None = None,
        line_number: int | None = None,
        column: int | None = None,
        suggestions: list[str] | None = None,
    ):
        self.message = message
        self.file_path = file_path
        self.line_number = line_number
        self.column = column
        self.suggestions = suggestions or []
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with context and suggestions."""
        lines = []

        # Header
        if self.file_path:
            lines.append(f"❌ Configuration Error in {self.file_path.name}")
        else:
            lines.append("❌ Configuration Error")

        # Location info
        if self.line_number:
            location = f"  Line {self.line_number}"
            if self.column:
                location += f", Column {self.column}"
            lines.append(location)

        # Main message
        lines.append(f"  {self.message}")

        # File context
        if self.file_path and self.line_number:
            context = self._get_file_context()
            if context:
                lines.extend(context)

        # Suggestions
        if self.suggestions:
            lines.append("")
            lines.append("  💡 Suggestions:")
            for suggestion in self.suggestions:
                lines.append(f"    • {suggestion}")

        return "\n".join(lines)

    def _get_file_context(self) -> list[str]:
        """Get surrounding lines from file for context."""
        if not self.file_path or not self.file_path.exists() or not self.line_number:
            return []

        try:
            with self.file_path.open() as f:
                file_lines = f.readlines()

            # Show 2 lines before and after
            start = max(0, self.line_number - 3)
            end = min(len(file_lines), self.line_number + 2)

            context_lines = ["", "  File context:"]
            for i in range(start, end):
                line_num = i + 1
                marker = "  > " if line_num == self.line_number else "    "
                line_content = file_lines[i].rstrip()

                # Highlight the error column if provided
                if line_num == self.line_number and self.column:
                    context_lines.append(f"{marker}{line_num:3} | {line_content}")
                    # Add arrow pointing to column
                    arrow_line = " " * (len(f"{marker}{line_num:3} | ") + self.column - 1) + "^"
                    context_lines.append(arrow_line)
                else:
                    context_lines.append(f"{marker}{line_num:3} | {line_content}")
        except OSError:
            return []
        else:
            return context_lines


class BlueprintNotFoundError(BlueprintError):
    """Blueprint not found error with suggestions."""

    def __init__(self, blueprint_name: str, available_blueprints: list[str] | None = None):
        self.blueprint_name = blueprint_name
        self.available_blueprints = available_blueprints or []

        suggestions = []

        # Find similar blueprint names
        if self.available_blueprints:
            similar = difflib.get_close_matches(
                blueprint_name, self.available_blueprints, n=3, cutoff=0.6
            )
            if similar:
                if len(similar) == 1:
                    suggestions.append(f"Did you mean '{similar[0]}'?")
                else:
                    similar_quoted = [f"'{s}'" for s in similar]
                    suggestions.append(f"Did you mean one of: {', '.join(similar_quoted)}?")

            suggestions.append(
                f"Available blueprints: {', '.join(sorted(self.available_blueprints))}"
            )
        else:
            suggestions.extend(
                [
                    "No blueprints found. Check that:",
                    "1. Your blueprint Python files exist in the dags directory",
                    "2. Your blueprint classes inherit from Blueprint[ConfigType]",
                    "3. The template directory path is correct",
                ]
            )

        message = f"Blueprint '{blueprint_name}' not found"
        super().__init__(
            f"{message}\n\n💡 Suggestions:\n" + "\n".join(f"  • {s}" for s in suggestions)
        )


class ValidationError(BlueprintError):
    """Enhanced validation error with better context."""

    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        expected_type: str | None = None,
        actual_value: Any | None = None,
        suggestions: list[str] | None = None,
    ):
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_value = actual_value

        if field_name:
            full_message = f"Validation failed for field '{field_name}': {message}"
        else:
            full_message = f"Validation failed: {message}"

        if expected_type and actual_value is not None:
            full_message += f"\n  Expected: {expected_type}"
            full_message += f"\n  Got: {type(actual_value).__name__} = {actual_value!r}"

        if suggestions:
            full_message += "\n\n💡 Suggestions:"
            for suggestion in suggestions:
                full_message += f"\n  • {suggestion}"

        super().__init__(full_message)


class YAMLParseError(ConfigurationError):
    """YAML parsing error with line number context."""

    @classmethod
    def from_yaml_error(cls, yaml_error: yaml.YAMLError, file_path: Path) -> "YAMLParseError":
        """Create from a yaml.YAMLError with extracted line information."""
        line_number = None
        column = None
        message = str(yaml_error)

        # Try to extract line number from yaml error
        if hasattr(yaml_error, "problem_mark"):
            mark = yaml_error.problem_mark
            line_number = mark.line + 1  # YAML uses 0-based indexing  # type: ignore[attr-defined]
            column = mark.column + 1  # type: ignore[attr-defined]

        # Extract problem description
        if hasattr(yaml_error, "problem"):
            message = yaml_error.problem or message

        suggestions = [
            "Check YAML syntax (proper indentation, quotes, etc.)",
            "Validate that all strings are properly quoted",
            "Ensure lists use '- ' prefix and maps use 'key: value' format",
        ]

        return cls(message, file_path, line_number, column, suggestions)


class DuplicateBlueprintError(BlueprintError):
    """Error when duplicate blueprint names are found."""

    def __init__(self, blueprint_name: str, locations: list[str]):
        self.blueprint_name = blueprint_name
        self.locations = locations
        # Pass raw args (not the rendered message) so repr() stays informative
        # while __str__ renders cwd-relative paths lazily at display time.
        super().__init__(blueprint_name, locations)

    def __str__(self) -> str:
        message = f"Duplicate blueprint name '{self.blueprint_name}' found in multiple locations:"
        for loc in self.locations:
            message += f"\n  • {display_path(loc)}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Rename one of the blueprint classes"
        message += "\n  • Use unique names for each blueprint"

        return message


class DuplicateDAGIdError(BlueprintError):
    """Error when duplicate DAG IDs are found across configurations."""

    def __init__(self, dag_id: str, config_files: list[Path]):
        self.dag_id = dag_id
        self.config_files = config_files

        message = f"Duplicate DAG ID '{dag_id}' found in multiple configuration files:"
        for config_file in config_files:
            message += f"\n  • {config_file.name}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Change the 'dag_id' field in one of the configuration files"
        message += "\n  • Use unique DAG IDs for each configuration"
        message += "\n  • Consider using a naming convention like '<team>-<service>-<purpose>'"

        super().__init__(message)


class CyclicDependencyError(BlueprintError):
    """Error when step dependencies form a cycle."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        cycle_display = " -> ".join(cycle)
        message = f"Cyclic dependency detected: {cycle_display}"
        message += "\n\n💡 Suggestions:"
        message += "\n  • Review the 'depends_on' fields in your DAG YAML"
        message += "\n  • Remove one of the dependencies to break the cycle"
        super().__init__(message)


class InvalidDependencyError(BlueprintError):
    """Error when a step references a non-existent dependency."""

    def __init__(self, step_name: str, invalid_dep: str, available_steps: list[str]):
        self.step_name = step_name
        self.invalid_dep = invalid_dep
        self.available_steps = available_steps

        message = f"Step '{step_name}' depends on '{invalid_dep}', which does not exist"

        if available_steps:
            similar = difflib.get_close_matches(invalid_dep, available_steps, n=3, cutoff=0.6)
            if similar:
                quoted = [f"'{s}'" for s in similar]
                message += f"\n  Did you mean: {', '.join(quoted)}?"
            message += f"\n  Available steps: {', '.join(sorted(available_steps))}"

        super().__init__(message)


class MultipleDagArgsError(BlueprintError):
    """Error when the DAG args template that applies to a DAG is ambiguous."""

    def __init__(
        self,
        candidates: dict[str, str],
        directory: str | None = None,
        for_path: str | None = None,
    ):
        self.candidates = candidates
        self.directory = directory
        self.for_path = for_path
        super().__init__(candidates, directory, for_path)

    def __str__(self) -> str:
        if self.directory:
            shown = display_path(self.directory)
            where = "the current directory" if shown == "." else shown
            message = f"Multiple BlueprintDagArgs templates are defined in {where}:"
        else:
            subject = f"'{display_path(self.for_path)}'" if self.for_path else "this DAG"
            message = (
                f"No BlueprintDagArgs template is defined in the directory of {subject} or any "
                "directory above it, and several are registered with none declared as the "
                "fallback:"
            )

        for name, loc in sorted(self.candidates.items()):
            suffix = f" ({display_path(loc)})" if loc else ""
            message += f"\n  • {name}{suffix}"

        message += "\n\n💡 Suggestions:"
        if self.directory:
            message += "\n  • Keep one template per directory"
            message += "\n  • Move the others into the directories whose DAGs use them"
        else:
            message += "\n  • Define a template in the DAG's directory or one above it"
            message += "\n  • Or declare a fallback: "
            message += "class MyDagArgs(BlueprintDagArgs[MyConfig], default=True)"

        return message


class MultipleDefaultDagArgsError(BlueprintError):
    """Error when more than one DAG args template is declared with default=True."""

    def __init__(self, candidates: dict[str, str]):
        self.candidates = candidates
        super().__init__(candidates)

    def __str__(self) -> str:
        message = "Multiple BlueprintDagArgs templates are declared with default=True:"
        for name, loc in sorted(self.candidates.items()):
            suffix = f" ({display_path(loc)})" if loc else ""
            message += f"\n  • {name}{suffix}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Keep default=True on exactly one template"
        message += "\n  • Templates in a parent directory of a DAG apply without being declared"

        return message


class DuplicateDagArgsError(BlueprintError):
    """Error when two DAG args templates share a name."""

    def __init__(self, name: str, locations: list[str]):
        self.name = name
        self.locations = locations
        super().__init__(name, locations)

    def __str__(self) -> str:
        message = f"Duplicate DAG args template '{self.name}' found in multiple locations:"
        for loc in self.locations:
            message += f"\n  • {display_path(loc)}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Give each template a distinct class name"
        message += '\n  • Or set name = "..." on one of them'

        return message


class DagArgsNotFoundError(BlueprintError):
    """Error when a requested DAG args template does not exist."""

    def __init__(self, name: str, available: list[str] | None = None):
        self.name = name
        self.available = available or []

        message = f"DAG args template '{name}' not found"
        if self.available:
            message += f"\n  Available templates: {', '.join(sorted(self.available))}"
        super().__init__(message)


class EntryPointLoadError(BlueprintError):
    """Error when a package advertising blueprints via entry points fails to load."""

    def __init__(self, entry_point: str, value: str, dist_name: str, cause: str):
        self.entry_point = entry_point
        self.value = value
        self.dist_name = dist_name
        self.cause = cause

        message = (
            f"Failed to load blueprints from entry point '{entry_point}' ({value}) "
            f"in package '{dist_name}': {cause}"
        )
        message += "\n\n💡 Suggestions:"
        message += f"\n  • Check that '{value}' is importable: python -c 'import {value}'"
        message += f"\n  • Verify '{dist_name}' and its dependencies are installed"
        message += "\n  • Pass skip_invalid_dags=True to log and skip instead of failing"

        super().__init__(message)


class UndefinedVariableError(BlueprintError):
    """Error when a ``${...}`` reference names a variable that does not exist."""

    def __init__(
        self,
        name: str,
        available: list[str],
        source: Any = None,
        detail: str | None = None,
    ):
        self.name = name
        self.available = available
        self.source = source
        self.detail = detail

        message = f"❌ Undefined variable '{name}'"
        if source:
            message += f" in {display_path(str(source))}"
        if detail:
            message += f"\n  {detail}"

        suggestions = []
        similar = difflib.get_close_matches(name, available, n=3, cutoff=0.6)
        if similar:
            quoted = [f"'{s}'" for s in similar]
            suggestions.append(f"Did you mean {' or '.join(quoted)}?")
        if available:
            shown = available[:MAX_SUGGESTION_VALUES]
            listed = ", ".join(shown)
            if len(available) > MAX_SUGGESTION_VALUES:
                listed += f", ... ({len(available)} total)"
            suggestions.append(f"Available variables: {listed}")
        else:
            suggestions.append("No variables are in scope for this file")

        suggestions.append(f"Declare it in a `vars:` block or {VARS_FILENAME_HINT}")
        suggestions.append(f"Or write `$${{{name}}}` if you meant the literal text `${{{name}}}`")

        message += "\n\n💡 Suggestions:"
        for suggestion in suggestions:
            message += f"\n  • {suggestion}"

        super().__init__(message)


class IncompleteVariableError(BlueprintError):
    """Error when a variable has no value under the active profile."""

    def __init__(self, name: str, profile: str, defined_for: list[str], source: Any = None):
        self.name = name
        self.profile = profile
        self.defined_for = defined_for
        self.source = source

        message = f"❌ Variable '{name}' has no value under profile '{profile}'"
        if source:
            message += f"\n  Defined in {display_path(str(source))} for: {', '.join(defined_for)}"

        message += "\n\n💡 Suggestions:"
        message += f"\n  • Add a '{profile}' entry for '{name}'"
        message += "\n  • Or give it a single value that applies to every profile"

        super().__init__(message)


class CyclicVariableError(BlueprintError):
    """Error when variables reference each other in a cycle."""

    def __init__(self, cycle: list[str], source: Any = None):
        self.cycle = cycle
        self.source = source

        message = f"❌ Cyclic variable reference: {' → '.join(cycle)}"
        if source:
            message += f"\n  In {display_path(str(source))}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Break the cycle by inlining one of the values"

        super().__init__(message)


class InvalidVariableValueError(BlueprintError):
    """Error when a variable value has a shape that is not allowed."""

    def __init__(
        self, name: str, reason: str, source: Any = None, suggestions: list[str] | None = None
    ):
        self.name = name
        self.reason = reason
        self.source = source

        message = f"❌ Invalid value for variable '{name}': {reason}"
        if source:
            message += f"\n  In {display_path(str(source))}"

        message += "\n\n💡 Suggestions:"
        for suggestion in suggestions or ["Use a scalar or a list of scalars"]:
            message += f"\n  • {suggestion}"

        super().__init__(message)


class CompositionDepthError(BlueprintError):
    """Error when variable composition nests deeper than the supported limit."""

    def __init__(self, chain: list[str], limit: int, source: Any = None):
        self.chain = chain
        self.limit = limit
        self.source = source

        message = f"❌ Variable composition nested more than {limit} levels deep"
        if source:
            message += f"\n  In {display_path(str(source))}"
        message += f"\n  Chain began: {' → '.join(chain[:5])} → ..."

        message += "\n\n💡 Suggestions:"
        message += "\n  • This is not a cycle; the chain is simply very long"
        message += "\n  • Inline some intermediate variables to shorten it"

        super().__init__(message)


class InvalidVariableNameError(BlueprintError):
    """Error when a variable name would collide with dotted namespace syntax."""

    def __init__(self, name: str, source: Any = None):
        self.name = name
        self.source = source

        message = f"❌ Invalid variable name '{name}'"
        if source:
            message += f" in {display_path(str(source))}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Names must start with a letter or underscore"
        message += "\n  • Use letters, digits, underscores and hyphens only"
        message += "\n  • Periods are reserved for future namespaces (e.g. ${env.FOO})"

        super().__init__(message)


class ProfileError(BlueprintError):
    """Error in profile declaration or selection."""

    def __init__(self, message: str, source: Any = None):
        self.source = source

        formatted = f"❌ {message}"
        if source:
            formatted += f"\n  In {display_path(str(source))}"

        super().__init__(formatted)


class InvalidVersionError(BlueprintError):
    """Error when a requested blueprint version does not exist."""

    def __init__(self, blueprint_name: str, requested_version: int, available_versions: list[int]):
        self.blueprint_name = blueprint_name
        self.requested_version = requested_version
        self.available_versions = available_versions

        message = f"Version {requested_version} of blueprint '{blueprint_name}' does not exist"
        if available_versions:
            versions_str = ", ".join(str(v) for v in sorted(available_versions))
            message += f"\n  Available versions: {versions_str}"
            message += f"\n  Latest version: {max(available_versions)}"

        super().__init__(message)


class NonContiguousVersionError(BlueprintError):
    """Error when a blueprint's versions don't form a contiguous 1..N sequence."""

    def __init__(self, blueprint_name: str, found_versions: list[int]):
        self.blueprint_name = blueprint_name
        self.found_versions = sorted(found_versions)

        expected = set(range(1, max(self.found_versions) + 1))
        missing = sorted(expected - set(self.found_versions))

        versions_str = ", ".join(str(v) for v in self.found_versions)
        missing_str = ", ".join(str(v) for v in missing)

        message = (
            f"Blueprint '{blueprint_name}' has non-contiguous versions: [{versions_str}]"
            f"\n  Missing versions: {missing_str}"
            f"\n  Versions must form a strict sequence from 1 to N"
        )
        super().__init__(message)


def suggest_valid_values(invalid_value: str, valid_values: list[str], field_name: str) -> list[str]:
    """Generate suggestions for invalid values.

    Args:
        invalid_value: The invalid value that was provided
        valid_values: List of valid values for the field
        field_name: Name of the field for context

    Returns:
        List of suggestion strings for error messages

    Example:
        ```python
        suggestions = suggest_valid_values(
            "hourli",
            ["hourly", "daily", "weekly"],
            "schedule"
        )
        # Returns: ["Did you mean 'hourly' for schedule?", "Valid values for schedule: daily, hourly, weekly"]
        ```
    """
    suggestions = []

    # Find close matches
    matches = difflib.get_close_matches(invalid_value, valid_values, n=3, cutoff=0.6)
    if matches:
        if len(matches) == 1:
            suggestions.append(f"Did you mean '{matches[0]}' for {field_name}?")
        else:
            matches_quoted = [f"'{m}'" for m in matches]
            suggestions.append(
                f"Did you mean one of: {', '.join(matches_quoted)} for {field_name}?"
            )

    # Show all valid values if not too many
    if len(valid_values) <= MAX_SUGGESTION_VALUES:
        suggestions.append(f"Valid values for {field_name}: {', '.join(sorted(valid_values))}")

    return suggestions
