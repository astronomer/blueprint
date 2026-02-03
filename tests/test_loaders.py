"""Tests for YAML loading and blueprint discovery."""

import pytest
from airflow import DAG

from blueprint import (
    discover_blueprints,
    from_yaml,
    get_blueprint_info,
    load_blueprint,
    render_yaml_template,
)
from blueprint.errors import BlueprintNotFoundError, ConfigurationError

# Constants
EXPECTED_RETRIES_OVERRIDE = 5
EXPECTED_BLUEPRINT_COUNT = 2
EXPECTED_DEFAULT_RETRIES = 2
EXPECTED_MAX_RETRIES = 5


class TestLoaders:
    """Test the loader functionality."""

    def test_load_blueprint_from_yaml(self, tmp_path):
        """Test loading a blueprint from YAML configuration."""

        # Create a test blueprint
        blueprint_code = """
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class TestConfig(BaseModel):
    job_id: str
    param1: str = "default"

class TestBlueprint(Blueprint[TestConfig]):
    def render(self, config: TestConfig) -> DAG:
        return DAG(
            dag_id=config.job_id,
            start_date=datetime(2024, 1, 1),
            tags=[config.param1]
        )
"""

        # Create template directory and file
        template_dir = tmp_path / ".astro" / "templates"
        template_dir.mkdir(parents=True)
        (template_dir / "test_blueprints.py").write_text(blueprint_code)

        # Create YAML config
        yaml_config = """
blueprint: test_blueprint
job_id: test-dag
param1: custom-value
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_config)

        # Load DAG from YAML
        dag = from_yaml(str(config_file), template_dir=str(template_dir))

        assert isinstance(dag, DAG)
        assert dag.dag_id == "test-dag"
        assert "custom-value" in dag.tags

    def test_yaml_with_overrides(self, tmp_path):
        """Test loading YAML with parameter overrides."""

        # Create a test blueprint
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class SimpleConfig(BaseModel):
    job_id: str
    retries: int = 2

class SimpleBlueprint(Blueprint[SimpleConfig]):
    def render(self, config: SimpleConfig) -> DAG:
        return DAG(
            dag_id=config.job_id,
            default_args={"retries": config.retries},
            start_date=datetime(2024, 1, 1)
        )
"""

        # Setup
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "simple.py").write_text(blueprint_code)

        yaml_content = """
blueprint: simple_blueprint
job_id: test-dag
retries: 3
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml_content)

        # Load with override
        dag = from_yaml(
            str(config_file),
            overrides={"retries": EXPECTED_RETRIES_OVERRIDE},
            template_dir=str(template_dir),
        )

        assert dag.default_args["retries"] == EXPECTED_RETRIES_OVERRIDE

    def test_discover_blueprints(self, tmp_path):
        """Test discovering available blueprints."""

        # Create multiple blueprints
        blueprint_code = '''
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class Config1(BaseModel):
    job_id: str

class FirstBlueprint(Blueprint[Config1]):
    """First test blueprint."""
    def render(self, config: Config1) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

class Config2(BaseModel):
    job_id: str
    schedule: str = "@daily"

class SecondBlueprint(Blueprint[Config2]):
    """Second test blueprint."""
    def render(self, config: Config2) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

# This should not be discovered
class NotABlueprint:
    pass
'''

        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_blueprints.py").write_text(blueprint_code)

        # Discover blueprints
        blueprints = discover_blueprints(str(template_dir))

        assert len(blueprints) == EXPECTED_BLUEPRINT_COUNT

        # Check blueprint names
        names = [bp["name"] for bp in blueprints]
        assert "first_blueprint" in names
        assert "second_blueprint" in names

        # Check descriptions
        first_bp = next(bp for bp in blueprints if bp["name"] == "first_blueprint")
        assert first_bp["description"] == "First test blueprint."
        assert first_bp["class"] == "FirstBlueprint"

    def test_get_blueprint_info(self, tmp_path):
        """Test getting detailed blueprint information."""

        blueprint_code = '''
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class DetailedConfig(BaseModel):
    job_id: str = Field(description="Unique job identifier")
    retries: int = Field(default=2, ge=0, le=5, description="Number of retries")
    enabled: bool = Field(default=True, description="Whether job is enabled")

class DetailedBlueprint(Blueprint[DetailedConfig]):
    """A blueprint with detailed configuration."""
    def render(self, config: DetailedConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
'''

        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "detailed.py").write_text(blueprint_code)

        # Get blueprint info
        info = get_blueprint_info("detailed_blueprint", str(template_dir))

        assert info["name"] == "detailed_blueprint"
        assert info["class"] == "DetailedBlueprint"
        assert "A blueprint with detailed configuration" in info["description"]

        # Check parameters
        params = info["parameters"]
        assert "job_id" in params
        assert params["job_id"]["description"] == "Unique job identifier"
        assert params["job_id"]["required"] is True

        assert "retries" in params
        assert params["retries"]["default"] == EXPECTED_DEFAULT_RETRIES
        assert params["retries"]["minimum"] == 0
        assert params["retries"]["maximum"] == EXPECTED_MAX_RETRIES

        # Check defaults
        assert info["defaults"] == {"retries": EXPECTED_DEFAULT_RETRIES, "enabled": True}

    def test_load_nonexistent_blueprint(self, tmp_path):
        """Test error when loading non-existent blueprint."""

        # Create empty template dir
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        with pytest.raises(BlueprintNotFoundError, match="Blueprint 'nonexistent' not found"):
            load_blueprint("nonexistent", str(template_dir))

    def test_yaml_missing_blueprint_field(self, tmp_path):
        """Test error when YAML is missing blueprint field."""

        yaml_content = """
job_id: test-dag
param1: value
"""
        config_file = tmp_path / "bad.yaml"
        config_file.write_text(yaml_content)

        with pytest.raises(ConfigurationError, match="Missing required field 'blueprint'"):
            from_yaml(str(config_file))

    def test_empty_yaml_file(self, tmp_path):
        """Test error with empty YAML file."""

        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        with pytest.raises(ConfigurationError, match="Configuration file is empty"):
            from_yaml(str(config_file))

    def test_from_yaml_with_template_rendering(self, tmp_path, monkeypatch):
        """Test from_yaml with Jinja2 template rendering enabled."""
        monkeypatch.setenv("TEST_ENV", "production")

        # Create a test blueprint
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class TemplatedConfig(BaseModel):
    job_id: str

class TemplatedBlueprint(Blueprint[TemplatedConfig]):
    def render(self, config: TemplatedConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""

        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "templated.py").write_text(blueprint_code)

        # Create YAML with Jinja2 template
        yaml_content = """
blueprint: templated_blueprint
job_id: "{{ env.TEST_ENV }}-job"
"""
        config_file = tmp_path / "templated.yaml"
        config_file.write_text(yaml_content)

        # Load with template rendering (default)
        dag = from_yaml(str(config_file), template_dir=str(template_dir))

        assert dag.dag_id == "production-job"

    def test_from_yaml_without_template_rendering(self, tmp_path, monkeypatch):
        """Test from_yaml with template rendering disabled uses literal values."""
        monkeypatch.setenv("TEST_ENV", "production")

        # Create a test blueprint that stores the env_value for verification
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class LiteralConfig(BaseModel):
    job_id: str
    env_value: str = ""

class LiteralBlueprint(Blueprint[LiteralConfig]):
    def render(self, config: LiteralConfig) -> DAG:
        dag = DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
        # Store env_value as a tag so we can verify it
        dag.tags = [config.env_value] if config.env_value else []
        return dag
"""

        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "literal.py").write_text(blueprint_code)

        # Create YAML - job_id is static, env_value uses template
        yaml_content = """
blueprint: literal_blueprint
job_id: static-job-id
env_value: "{{ env.TEST_ENV }}"
"""
        config_file = tmp_path / "literal.yaml"
        config_file.write_text(yaml_content)

        # Load without template rendering
        dag = from_yaml(
            str(config_file),
            template_dir=str(template_dir),
            render_template=False,
        )

        # The job_id should be the static value
        assert dag.dag_id == "static-job-id"
        # The env_value should NOT be rendered (literal template string)
        assert "{{ env.TEST_ENV }}" in dag.tags


class TestRenderYamlTemplate:
    """Tests for render_yaml_template function."""

    def test_basic_rendering(self, tmp_path):
        """Test basic YAML rendering without templates."""
        yaml_content = """
blueprint: test
job_id: simple-job
schedule: "@daily"
"""
        config_file = tmp_path / "simple.yaml"
        config_file.write_text(yaml_content)

        config, rendered = render_yaml_template(config_file)

        assert config["blueprint"] == "test"
        assert config["job_id"] == "simple-job"
        assert config["schedule"] == "@daily"

    def test_env_variable_template(self, tmp_path, monkeypatch):
        """Test rendering with environment variable."""
        monkeypatch.setenv("MY_ENV", "production")

        yaml_content = """
blueprint: test
job_id: "{{ env.MY_ENV }}-job"
"""
        config_file = tmp_path / "env.yaml"
        config_file.write_text(yaml_content)

        config, rendered = render_yaml_template(config_file)

        assert config["job_id"] == "production-job"
        assert "production-job" in rendered

    def test_default_filter(self, tmp_path):
        """Test the default filter for fallback values."""
        yaml_content = """
blueprint: test
schedule: "{{ var.value.missing_key | default('@daily') }}"
"""
        config_file = tmp_path / "default.yaml"
        config_file.write_text(yaml_content)

        config, rendered = render_yaml_template(config_file)

        assert config["schedule"] == "@daily"

    def test_custom_context(self, tmp_path):
        """Test rendering with custom context."""
        yaml_content = """
blueprint: test
job_id: "{{ custom_var }}-job"
"""
        config_file = tmp_path / "custom.yaml"
        config_file.write_text(yaml_content)

        config, rendered = render_yaml_template(
            config_file,
            context={"custom_var": "my_value"},
        )

        assert config["job_id"] == "my_value-job"

    def test_disable_airflow_context(self, tmp_path, monkeypatch):
        """Test rendering with Airflow context disabled."""
        monkeypatch.setenv("MY_ENV", "production")

        yaml_content = """
blueprint: test
job_id: "{{ env.MY_ENV }}-job"
"""
        config_file = tmp_path / "no_airflow.yaml"
        config_file.write_text(yaml_content)

        # With Airflow context disabled, env should not be available
        # This should raise an error since env is undefined
        with pytest.raises(ConfigurationError, match="Template rendering failed"):
            render_yaml_template(config_file, use_airflow_context=False)

    def test_invalid_template_syntax(self, tmp_path):
        """Test error handling for invalid Jinja2 syntax."""
        yaml_content = """
blueprint: test
job_id: "{{ invalid syntax here"
"""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml_content)

        with pytest.raises(ConfigurationError, match="Template rendering failed"):
            render_yaml_template(config_file)

    def test_empty_file(self, tmp_path):
        """Test error handling for empty file."""
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        with pytest.raises(ConfigurationError, match="empty"):
            render_yaml_template(config_file)

    def test_nonexistent_file(self, tmp_path):
        """Test error handling for nonexistent file."""
        config_file = tmp_path / "nonexistent.yaml"

        with pytest.raises(ConfigurationError, match="Failed to read"):
            render_yaml_template(config_file)

    def test_sandboxed_environment_blocks_dangerous_operations(self, tmp_path):
        """Test that the Jinja2 sandbox blocks dangerous template operations."""
        from jinja2.exceptions import SecurityError

        # Attempt to access __class__ which should be blocked by sandbox
        dangerous_yaml = """
blueprint: test
job_id: "{{ ''.__class__.__mro__[1].__subclasses__() }}"
"""
        config_file = tmp_path / "dangerous.yaml"
        config_file.write_text(dangerous_yaml)

        # Should raise SecurityError or ConfigurationError wrapping it
        with pytest.raises((SecurityError, ConfigurationError)):
            render_yaml_template(config_file, use_airflow_context=False)
