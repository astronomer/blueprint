"""Tests for core Blueprint functionality."""

import inspect
from datetime import datetime, timezone

import pytest
from airflow import DAG
from pydantic import BaseModel, Field

from blueprint import Blueprint

# Constants
EXPECTED_RETRIES_VALUE = 3
EXPECTED_DEFAULT_RETRIES = 2


class TestBlueprint:
    """Test the core Blueprint class functionality."""

    def test_basic_blueprint(self):
        """Test creating a basic Blueprint with automatic build method."""

        # Define a config model
        class SimpleConfig(BaseModel):
            job_id: str
            schedule: str = "@daily"

        # Define a Blueprint
        class SimpleBlueprint(Blueprint[SimpleConfig]):
            def render(self, config: SimpleConfig) -> DAG:
                return DAG(
                    dag_id=config.job_id,
                    schedule=config.schedule,
                    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                    catchup=False,
                )

        # Test that build method exists
        assert hasattr(SimpleBlueprint, "build")
        assert callable(SimpleBlueprint.build)

        # Test building a DAG
        dag = SimpleBlueprint.build(job_id="test_dag")
        assert isinstance(dag, DAG)
        assert dag.dag_id == "test_dag"
        # In Airflow 3.x, it's schedule not schedule_interval
        assert dag.schedule == "@daily"

    def test_blueprint_with_validation(self):
        """Test Blueprint with Pydantic validation."""

        class ValidatedConfig(BaseModel):
            job_id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$")
            retries: int = Field(ge=0, le=5)

        class ValidatedBlueprint(Blueprint[ValidatedConfig]):
            def render(self, config: ValidatedConfig) -> DAG:
                return DAG(
                    dag_id=config.job_id,
                    default_args={"retries": config.retries},
                    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                )

        # Valid config should work
        dag = ValidatedBlueprint.build(job_id="valid_dag", retries=EXPECTED_RETRIES_VALUE)
        assert dag.dag_id == "valid_dag"
        assert dag.default_args["retries"] == EXPECTED_RETRIES_VALUE

        # Invalid job_id should raise validation error
        with pytest.raises(ValueError):  # Pydantic will raise validation error
            ValidatedBlueprint.build(job_id="invalid dag!", retries=EXPECTED_RETRIES_VALUE)

        # Invalid retries should raise validation error
        with pytest.raises(ValueError):
            ValidatedBlueprint.build(job_id="valid_dag", retries=10)

    def test_blueprint_method_signature(self):
        """Test that build method has proper signature for IDE support."""

        class TypedConfig(BaseModel):
            job_id: str
            source_table: str
            schedule: str = "@daily"
            retries: int = 2

        class TypedBlueprint(Blueprint[TypedConfig]):
            def render(self, config: TypedConfig) -> DAG:
                return DAG(
                    dag_id=config.job_id, start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)
                )

        # Check signature - for classmethod, inspect the __func__ attribute
        sig = inspect.signature(TypedBlueprint.build.__func__)
        params = list(sig.parameters.keys())

        # Should have cls plus all config parameters
        assert params[0] == "cls"
        assert "job_id" in params
        assert "source_table" in params
        assert "schedule" in params
        assert "retries" in params

        # Check defaults are preserved
        assert sig.parameters["schedule"].default == "@daily"
        assert sig.parameters["retries"].default == EXPECTED_DEFAULT_RETRIES

    def test_get_schema(self):
        """Test JSON Schema generation."""

        class SchemaConfig(BaseModel):
            job_id: str = Field(description="Unique job ID")
            enabled: bool = Field(default=True, description="Whether job is enabled")

        class SchemaBlueprint(Blueprint[SchemaConfig]):
            def render(self, config: SchemaConfig) -> DAG:
                return DAG(
                    dag_id=config.job_id, start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)
                )

        schema = SchemaBlueprint.get_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "job_id" in schema["properties"]
        assert schema["properties"]["job_id"]["type"] == "string"
        assert "enabled" in schema["properties"]
        assert schema["properties"]["enabled"]["default"] is True

    def test_render_not_implemented(self):
        """Test that render() must be implemented."""

        class NoRenderConfig(BaseModel):
            job_id: str

        class NoRenderBlueprint(Blueprint[NoRenderConfig]):
            pass  # No render method

        with pytest.raises(NotImplementedError):
            NoRenderBlueprint.build(job_id="test")
