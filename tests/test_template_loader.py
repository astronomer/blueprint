"""Tests for the template loader with duplicate DAG ID detection."""

import pytest

from blueprint import DuplicateDAGIdError
from blueprint.template_loader import discover_yaml_dags


class TestDiscoverYAMLDAGs:
    """Test YAML DAG discovery with duplicate detection."""

    def test_discover_yaml_dags_no_duplicates(self, tmp_path):
        """Test normal DAG discovery without duplicates."""
        # Create template with a simple blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class TestConfig(BaseModel):
    job_id: str
    schedule: str = "@daily"

class TestBlueprint(Blueprint[TestConfig]):
    def render(self, config: TestConfig) -> DAG:
        return DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1)
        )
"""
        (template_dir / "test_blueprint.py").write_text(blueprint_code)

        # Create configs directory with different DAG IDs
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        config1 = """
blueprint: test_blueprint
job_id: customer-etl
schedule: "@daily"
"""
        (configs_dir / "customer.dag.yaml").write_text(config1)

        config2 = """
blueprint: test_blueprint
job_id: sales-etl
schedule: "@hourly"
"""
        (configs_dir / "sales.dag.yaml").write_text(config2)

        config3 = """
blueprint: test_blueprint
job_id: nested-etl
schedule: "@hourly"
"""
        nested_dir = configs_dir / "nested"
        nested_dir.mkdir()
        (nested_dir / "nested.dag.yaml").write_text(config3)
        # Discover DAGs - should work without errors
        dags = discover_yaml_dags(configs_dir=str(configs_dir), template_dir=str(template_dir))

        assert len(dags) == 3
        assert "customer" in dags
        assert "sales" in dags
        assert "nested" in dags
        assert dags["customer"].dag_id == "customer-etl"
        assert dags["sales"].dag_id == "sales-etl"
        assert dags["nested"].dag_id == "nested-etl"

    def test_discover_yaml_dags_with_duplicates(self, tmp_path):
        """Test DAG discovery that detects duplicate DAG IDs."""
        # Create template with a simple blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class TestConfig(BaseModel):
    job_id: str
    schedule: str = "@daily"

class TestBlueprint(Blueprint[TestConfig]):
    def render(self, config: TestConfig) -> DAG:
        return DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1)
        )
"""
        (template_dir / "test_blueprint.py").write_text(blueprint_code)

        # Create configs directory with DUPLICATE DAG IDs
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        config1 = """
blueprint: test_blueprint
job_id: duplicate-dag-id
schedule: "@daily"
"""
        (configs_dir / "customer.dag.yaml").write_text(config1)

        config2 = """
blueprint: test_blueprint
job_id: duplicate-dag-id
schedule: "@hourly"
"""
        (configs_dir / "sales.dag.yaml").write_text(config2)

        # Discover DAGs - should raise DuplicateDAGIdError
        with pytest.raises(DuplicateDAGIdError) as exc_info:
            discover_yaml_dags(configs_dir=str(configs_dir), template_dir=str(template_dir))

        error = exc_info.value
        assert error.dag_id == "duplicate-dag-id"
        assert len(error.config_files) == 2
        assert any(f.name == "customer.dag.yaml" for f in error.config_files)
        assert any(f.name == "sales.dag.yaml" for f in error.config_files)

    def test_discover_yaml_dags_partial_duplicates(self, tmp_path):
        """Test DAG discovery where only some DAGs have duplicate IDs."""
        # Create template with a simple blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class TestConfig(BaseModel):
    job_id: str
    schedule: str = "@daily"

class TestBlueprint(Blueprint[TestConfig]):
    def render(self, config: TestConfig) -> DAG:
        return DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1)
        )
"""
        (template_dir / "test_blueprint.py").write_text(blueprint_code)

        # Create configs directory with one duplicate and one unique
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        config1 = """
blueprint: test_blueprint
job_id: unique-dag
schedule: "@daily"
"""
        (configs_dir / "unique.dag.yaml").write_text(config1)

        config2 = """
blueprint: test_blueprint
job_id: duplicate-dag
schedule: "@hourly"
"""
        (configs_dir / "first_duplicate.dag.yaml").write_text(config2)

        config3 = """
blueprint: test_blueprint
job_id: duplicate-dag
schedule: "@weekly"
"""
        (configs_dir / "second_duplicate.dag.yaml").write_text(config3)

        # Discover DAGs - should raise DuplicateDAGIdError
        with pytest.raises(DuplicateDAGIdError) as exc_info:
            discover_yaml_dags(configs_dir=str(configs_dir), template_dir=str(template_dir))

        error = exc_info.value
        assert error.dag_id == "duplicate-dag"
        assert len(error.config_files) == 2
        # The unique DAG should not be mentioned in the error

    def test_discover_yaml_dags_empty_directory(self, tmp_path):
        """Test DAG discovery with empty configs directory."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        # Should return empty dict, no error
        dags = discover_yaml_dags(configs_dir=str(configs_dir), template_dir=str(template_dir))

        assert len(dags) == 0

    def test_discover_yaml_dags_nonexistent_directory(self, tmp_path):
        """Test DAG discovery with non-existent configs directory."""
        configs_dir = tmp_path / "nonexistent"
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        # Should return empty dict, no error
        dags = discover_yaml_dags(configs_dir=str(configs_dir), template_dir=str(template_dir))

        assert len(dags) == 0
