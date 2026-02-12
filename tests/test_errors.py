"""Tests for the Blueprint error system."""

import yaml

from blueprint.errors import (
    BlueprintNotFoundError,
    ConfigurationError,
    CyclicDependencyError,
    DuplicateBlueprintError,
    DuplicateDAGIdError,
    InvalidDependencyError,
    InvalidVersionError,
    ValidationError,
    YAMLParseError,
    suggest_valid_values,
)


class TestConfigurationError:
    """Test ConfigurationError with rich context."""

    def test_basic_error_message(self):
        error = ConfigurationError("Something went wrong")
        message = str(error)
        assert "Configuration Error" in message
        assert "Something went wrong" in message

    def test_error_with_file_path(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        error = ConfigurationError("Invalid configuration", file_path=config_file)
        message = str(error)
        assert "test.yaml" in message
        assert "Invalid configuration" in message

    def test_error_with_line_number(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        config_file.write_text("blueprint: test\njob_id: my-job\ninvalid_field: value\n")

        error = ConfigurationError(
            "Unknown field 'invalid_field'",
            file_path=config_file,
            line_number=3,
            column=1,
        )
        message = str(error)
        assert "Line 3" in message
        assert "Column 1" in message

    def test_error_with_suggestions(self):
        error = ConfigurationError(
            "Missing required field",
            suggestions=["Add 'blueprint' field", "Check documentation"],
        )
        message = str(error)
        assert "Suggestions:" in message
        assert "Add 'blueprint' field" in message


class TestBlueprintNotFoundError:
    """Test BlueprintNotFoundError with suggestions."""

    def test_no_blueprints_available(self):
        error = BlueprintNotFoundError("my_blueprint")
        message = str(error)
        assert "Blueprint 'my_blueprint' not found" in message

    def test_with_available_blueprints(self):
        error = BlueprintNotFoundError(
            "daily_etl", available_blueprints=["hourly_etl", "weekly_etl", "daily_export"]
        )
        message = str(error)
        assert "Blueprint 'daily_etl' not found" in message

    def test_fuzzy_matching_suggestions(self):
        error = BlueprintNotFoundError(
            "dayli_etl",
            available_blueprints=["daily_etl", "hourly_etl", "weekly_etl"],
        )
        message = str(error)
        assert "daily_etl" in message


class TestCyclicDependencyError:
    """Test cycle detection error."""

    def test_cycle_message(self):
        error = CyclicDependencyError(["a", "b", "c", "a"])
        message = str(error)
        assert "Cyclic dependency detected" in message
        assert "a -> b -> c -> a" in message
        assert "depends_on" in message


class TestInvalidDependencyError:
    """Test invalid dependency reference error."""

    def test_basic_message(self):
        error = InvalidDependencyError("transform", "extrat", ["extract", "load"])
        message = str(error)
        assert "transform" in message
        assert "extrat" in message
        assert "does not exist" in message

    def test_fuzzy_suggestion(self):
        error = InvalidDependencyError("transform", "extrat", ["extract", "load"])
        message = str(error)
        assert "extract" in message


class TestInvalidVersionError:
    """Test invalid version error."""

    def test_basic_message(self):
        error = InvalidVersionError("extract", 5, [1, 2])
        message = str(error)
        assert "Version 5" in message
        assert "extract" in message
        assert "does not exist" in message
        assert "1, 2" in message
        assert "Latest version: 2" in message

    def test_no_versions(self):
        error = InvalidVersionError("extract", 1, [])
        message = str(error)
        assert "Version 1" in message


class TestYAMLParseError:
    """Test YAML parsing error handling."""

    def test_from_yaml_error(self, tmp_path):
        yaml_content = "blueprint: test\njob_id: test\n  invalid_indent: true\n"
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml_content)

        try:
            with config_file.open() as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            error = YAMLParseError.from_yaml_error(e, config_file)
            message = str(error)
            assert "invalid.yaml" in message


class TestDuplicateBlueprintError:
    """Test duplicate blueprint error."""

    def test_duplicate_locations(self):
        error = DuplicateBlueprintError(
            "my_blueprint",
            locations=["templates/etl.py", "templates/pipelines/etl.py"],
        )
        message = str(error)
        assert "Duplicate blueprint name 'my_blueprint'" in message


class TestDuplicateDAGIdError:
    """Test duplicate DAG ID error."""

    def test_duplicate_dag_id_error(self, tmp_path):
        config1 = tmp_path / "customer_etl.dag.yaml"
        config2 = tmp_path / "sales_etl.dag.yaml"
        error = DuplicateDAGIdError("my-dag-id", [config1, config2])
        message = str(error)
        assert "Duplicate DAG ID 'my-dag-id'" in message
        assert "customer_etl.dag.yaml" in message
        assert "sales_etl.dag.yaml" in message


class TestValidationError:
    """Test enhanced ValidationError."""

    def test_basic_message(self):
        error = ValidationError("invalid value")
        assert "Validation failed: invalid value" in str(error)

    def test_with_field_name(self):
        error = ValidationError("must be positive", field_name="count")
        message = str(error)
        assert "field 'count'" in message
        assert "must be positive" in message

    def test_with_type_mismatch(self):
        error = ValidationError(
            "wrong type",
            field_name="batch_size",
            expected_type="int",
            actual_value="not_a_number",
        )
        message = str(error)
        assert "Expected: int" in message
        assert "str" in message
        assert "not_a_number" in message

    def test_with_suggestions(self):
        error = ValidationError(
            "invalid",
            suggestions=["Try 'append'", "Try 'overwrite'"],
        )
        message = str(error)
        assert "Try 'append'" in message
        assert "Try 'overwrite'" in message


class TestSuggestionHelpers:
    """Test suggestion helper functions."""

    def test_suggest_valid_values(self):
        suggestions = suggest_valid_values(
            "hourli", ["hourly", "daily", "weekly", "monthly"], "schedule"
        )
        assert any("hourly" in s for s in suggestions)

    def test_suggest_multiple_matches(self):
        suggestions = suggest_valid_values(
            "daily_", ["daily_full", "daily_incremental", "daily_backup"], "pattern"
        )
        assert len(suggestions) > 0
