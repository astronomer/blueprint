"""Blueprint error handling with rich context and suggestions."""

import difflib
from pathlib import Path
from typing import Any, List, Optional

import yaml

# Constants
MAX_SUGGESTION_VALUES = 10


class BlueprintError(Exception):
    """Base exception for all blueprint errors."""


class ConfigurationError(BlueprintError):
    """Configuration-related errors with rich context."""

    def __init__(
        self,
        message: str,
        file_path: Optional[Path] = None,
        line_number: Optional[int] = None,
        column: Optional[int] = None,
        suggestions: Optional[List[str]] = None,
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

    def _get_file_context(self) -> List[str]:
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
        except Exception:
            return []
        else:
            return context_lines


class BlueprintNotFoundError(BlueprintError):
    """Blueprint not found error with suggestions."""

    def __init__(self, blueprint_name: str, available_blueprints: Optional[List[str]] = None):
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
                    "1. Your Blueprint files are in the dags/ directory",
                    "2. Your blueprint classes inherit from Blueprint[ConfigType]",
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
        field_name: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_value: Optional[Any] = None,
        suggestions: Optional[List[str]] = None,
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

    def __init__(self, blueprint_name: str, locations: List[str]):
        self.blueprint_name = blueprint_name
        self.locations = locations

        message = f"Duplicate blueprint name '{blueprint_name}' found in multiple locations:"
        for loc in locations:
            message += f"\n  • {loc}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Rename one of the blueprint classes"
        message += "\n  • Use unique names for each blueprint"

        super().__init__(message)


class DuplicateDAGIdError(BlueprintError):
    """Error when duplicate DAG IDs are found across configurations."""

    def __init__(self, dag_id: str, config_files: List[Path]):
        self.dag_id = dag_id
        self.config_files = config_files

        message = f"Duplicate DAG ID '{dag_id}' found in multiple configuration files:"
        for config_file in config_files:
            message += f"\n  • {config_file.name}"

        message += "\n\n💡 Suggestions:"
        message += "\n  • Change the 'job_id' field in one of the configuration files"
        message += "\n  • Use unique DAG IDs for each configuration"
        message += "\n  • Consider using a naming convention like '<team>-<service>-<purpose>'"

        super().__init__(message)


def suggest_valid_values(invalid_value: str, valid_values: List[str], field_name: str) -> List[str]:
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
