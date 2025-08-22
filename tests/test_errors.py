"""Tests for the Blueprint error system."""

import yaml

from blueprint.errors import (
    BlueprintNotFoundError,
    ConfigurationError,
    DuplicateBlueprintError,
    DuplicateDAGIdError,
    YAMLParseError,
    suggest_valid_values,
)


class TestConfigurationError:
    """Test ConfigurationError with rich context."""

    def test_basic_error_message(self):
        """Test basic error message formatting."""
        error = ConfigurationError("Something went wrong")
        message = str(error)
        assert "❌ Configuration Error" in message
        assert "Something went wrong" in message

    def test_error_with_file_path(self, tmp_path):
        """Test error with file path."""
        config_file = tmp_path / "test.yaml"
        error = ConfigurationError("Invalid configuration", file_path=config_file)
        message = str(error)
        assert "test.yaml" in message
        assert "Invalid configuration" in message

    def test_error_with_line_number(self, tmp_path):
        """Test error with line number and file context."""
        # Create a test file
        config_file = tmp_path / "test.yaml"
        config_file.write_text("""blueprint: test
job_id: my-job
invalid_field: value
schedule: "@daily"
""")

        error = ConfigurationError(
            "Unknown field 'invalid_field'",
            file_path=config_file,
            line_number=3,
            column=1,
        )
        message = str(error)

        # Check line number is shown
        assert "Line 3" in message
        assert "Column 1" in message

        # Check file context is shown
        assert "invalid_field: value" in message
        assert ">" in message  # Current line marker

    def test_error_with_suggestions(self):
        """Test error with suggestions."""
        error = ConfigurationError(
            "Missing required field",
            suggestions=[
                "Add 'blueprint' field to your configuration",
                "Check the documentation for required fields",
            ],
        )
        message = str(error)

        assert "💡 Suggestions:" in message
        assert "Add 'blueprint' field" in message
        assert "Check the documentation" in message


class TestBlueprintNotFoundError:
    """Test BlueprintNotFoundError with suggestions."""

    def test_no_blueprints_available(self):
        """Test error when no blueprints are available."""
        error = BlueprintNotFoundError("my_blueprint")
        message = str(error)

        assert "Blueprint 'my_blueprint' not found" in message
        assert "No blueprints found" in message
        assert "templates directory exists" in message

    def test_with_available_blueprints(self):
        """Test error with available blueprints."""
        error = BlueprintNotFoundError(
            "daily_etl", available_blueprints=["hourly_etl", "weekly_etl", "daily_export"]
        )
        message = str(error)

        assert "Blueprint 'daily_etl' not found" in message
        assert "Available blueprints:" in message
        assert "hourly_etl" in message

    def test_fuzzy_matching_suggestions(self):
        """Test fuzzy matching for similar blueprint names."""
        error = BlueprintNotFoundError(
            "dayli_etl",  # Typo in 'daily'
            available_blueprints=["daily_etl", "hourly_etl", "weekly_etl"],
        )
        message = str(error)

        assert "Did you mean 'daily_etl'?" in message


class TestYAMLParseError:
    """Test YAML parsing error handling."""

    def test_from_yaml_error(self, tmp_path):
        """Test creating from a yaml.YAMLError."""

        # Create invalid YAML
        yaml_content = """
blueprint: test
job_id: test
  invalid_indent: true
"""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml_content)

        # Try to parse and catch error
        try:
            with config_file.open() as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            error = YAMLParseError.from_yaml_error(e, config_file)
            message = str(error)

            assert "invalid.yaml" in message
            assert "Line" in message  # Should extract line number
            assert "Check YAML syntax" in message


class TestDuplicateBlueprintError:
    """Test duplicate blueprint error."""

    def test_duplicate_locations(self):
        """Test error showing duplicate locations."""
        error = DuplicateBlueprintError(
            "my_blueprint",
            locations=[
                ".astro/templates/etl.py",
                ".astro/templates/pipelines/etl.py",
            ],
        )
        message = str(error)

        assert "Duplicate blueprint name 'my_blueprint'" in message
        assert ".astro/templates/etl.py" in message
        assert ".astro/templates/pipelines/etl.py" in message
        assert "Rename one of the blueprint classes" in message


class TestDuplicateDAGIdError:
    """Test duplicate DAG ID error."""

    def test_duplicate_dag_id_error(self, tmp_path):
        """Test error showing duplicate DAG IDs."""
        # Create test config files
        config1 = tmp_path / "customer_etl.dag.yaml"
        config2 = tmp_path / "sales_etl.dag.yaml"

        error = DuplicateDAGIdError("my-dag-id", [config1, config2])
        message = str(error)

        assert "Duplicate DAG ID 'my-dag-id'" in message
        assert "customer_etl.dag.yaml" in message
        assert "sales_etl.dag.yaml" in message
        assert "Change the 'job_id' field" in message
        assert "Use unique DAG IDs" in message
        assert "naming convention" in message

    def test_duplicate_dag_id_error_single_file_fallback(self, tmp_path):
        """Test error with single file (edge case)."""
        config1 = tmp_path / "test.dag.yaml"

        error = DuplicateDAGIdError("test-id", [config1])
        message = str(error)

        assert "Duplicate DAG ID 'test-id'" in message
        assert "test.dag.yaml" in message


class TestSuggestionHelpers:
    """Test suggestion helper functions."""

    def test_suggest_valid_values(self):
        """Test value suggestion helper."""
        suggestions = suggest_valid_values(
            "hourli", ["hourly", "daily", "weekly", "monthly"], "schedule"
        )

        assert any("Did you mean 'hourly'" in s for s in suggestions)
        assert any("Valid values for schedule:" in s for s in suggestions)

    def test_suggest_multiple_matches(self):
        """Test multiple close matches."""
        suggestions = suggest_valid_values(
            "daily_", ["daily_full", "daily_incremental", "daily_backup"], "pattern"
        )

        assert any("Did you mean one of:" in s for s in suggestions)
