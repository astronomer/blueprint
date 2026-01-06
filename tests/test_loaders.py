"""Tests for YAML loading and blueprint discovery."""

import pytest
from airflow import DAG

from blueprint import (
    discover_blueprints,
    from_yaml,
    get_blueprint_info,
    load_blueprint,
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
        template_dir = tmp_path / "blueprint" / "templates"
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
