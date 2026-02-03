"""Tests for the Blueprint registry system."""

import pytest

from blueprint import Blueprint
from blueprint.errors import BlueprintNotFoundError, DuplicateBlueprintError
from blueprint.registry import BlueprintRegistry


class TestBlueprintRegistry:
    """Test the BlueprintRegistry functionality."""

    @pytest.fixture
    def registry(self):
        """Create a fresh registry for each test."""
        reg = BlueprintRegistry()
        reg.clear()
        return reg

    @pytest.fixture
    def temp_blueprints(self, tmp_path):
        """Create temporary blueprint files for testing."""
        # Create templates directory
        template_dir = tmp_path / ".astro" / "templates"
        template_dir.mkdir(parents=True)

        # Create a simple blueprint
        simple_bp = template_dir / "simple.py"
        simple_bp.write_text(
            """
from blueprint import Blueprint, BaseModel

class SimpleConfig(BaseModel):
    job_id: str

class SimpleBlueprint(Blueprint[SimpleConfig]):
    '''A simple test blueprint.'''
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        )

        # Create another blueprint
        etl_bp = template_dir / "etl.py"
        etl_bp.write_text(
            """
from blueprint import Blueprint, BaseModel

class ETLConfig(BaseModel):
    job_id: str
    source_table: str

class ETLBlueprint(Blueprint[ETLConfig]):
    '''An ETL blueprint.'''
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

class DailyETL(Blueprint[ETLConfig]):
    '''Daily ETL variant.'''
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        )

        return template_dir

    def test_get_template_dirs_default(self, registry):
        """Test default template directory discovery."""
        dirs = registry.get_template_dirs()

        # Should include default dags/ path
        assert any(str(d).endswith("/dags") or str(d) == "dags" for d in dirs)

    def test_discover_blueprints(self, registry, temp_blueprints, monkeypatch):
        """Test blueprint discovery."""
        # Override get_template_dirs to use our temp directory
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])

        registry.discover_blueprints()

        # Check blueprints were discovered
        blueprints = registry.list_blueprints()
        names = [bp["name"] for bp in blueprints]

        assert "simple_blueprint" in names
        assert "etl_blueprint" in names
        assert "daily_etl" in names

    def test_get_blueprint(self, registry, temp_blueprints, monkeypatch):
        """Test getting a blueprint by name."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])
        registry.discover_blueprints()

        # Get blueprint
        bp_class = registry.get_blueprint("simple_blueprint")
        assert bp_class.__name__ == "SimpleBlueprint"
        assert issubclass(bp_class, Blueprint)

    def test_get_blueprint_not_found(self, registry, temp_blueprints, monkeypatch):
        """Test error when blueprint not found."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])
        registry.discover_blueprints()

        with pytest.raises(BlueprintNotFoundError) as exc_info:
            registry.get_blueprint("nonexistent_blueprint")

        error = exc_info.value
        assert "nonexistent_blueprint" in str(error)
        assert "simple_blueprint" in str(error)  # Should list available

    def test_duplicate_blueprint_detection(self, registry, tmp_path, monkeypatch):
        """Test detection of duplicate blueprint names."""
        # Create two directories with same blueprint name
        dir1 = tmp_path / "templates1"
        dir2 = tmp_path / "templates2"
        dir1.mkdir()
        dir2.mkdir()

        # Same blueprint name in both
        for d in [dir1, dir2]:
            (d / "test.py").write_text(
                """
from blueprint import Blueprint, BaseModel

class Config(BaseModel):
    job_id: str

class TestBlueprint(Blueprint[Config]):
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
            )

        # Override template dirs
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [dir1, dir2])
        registry.discover_blueprints(force=True)

        # Should detect duplicate
        with pytest.raises(DuplicateBlueprintError) as exc_info:
            registry.get_blueprint("test_blueprint")

        error = exc_info.value
        assert "test_blueprint" in str(error)
        assert "templates1/test.py" in str(error)
        assert "templates2/test.py" in str(error)

    def test_list_blueprints_with_metadata(self, registry, temp_blueprints, monkeypatch):
        """Test listing blueprints with full metadata."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])
        registry.discover_blueprints()

        blueprints = registry.list_blueprints()

        # Find simple blueprint
        simple = next(bp for bp in blueprints if bp["name"] == "simple_blueprint")

        assert simple["class"] == "SimpleBlueprint"
        assert "simple test blueprint" in simple["description"]
        assert "schema" in simple
        # list_blueprints() uses AST parsing and doesn't load full schema
        # Use get_blueprint_info() for full schema
        assert simple["schema"] == {}

    def test_get_blueprint_info(self, registry, temp_blueprints, monkeypatch):
        """Test getting detailed blueprint information."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])
        registry.discover_blueprints()

        info = registry.get_blueprint_info("etl_blueprint")

        assert info["name"] == "etl_blueprint"
        assert info["class"] == "ETLBlueprint"
        assert "ETL blueprint" in info["description"]

        # Check parameters
        params = info["parameters"]
        assert "job_id" in params
        assert "source_table" in params
        assert params["job_id"]["required"] is True
        assert params["source_table"]["required"] is True

    def test_force_rediscovery(self, registry, temp_blueprints, monkeypatch):
        """Test force rediscovery of blueprints."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])

        # First discovery
        registry.discover_blueprints()
        initial_count = len(registry.list_blueprints())

        # Add a new blueprint
        new_bp = temp_blueprints / "new.py"
        new_bp.write_text(
            """
from blueprint import Blueprint, BaseModel

class NewConfig(BaseModel):
    job_id: str

class NewBlueprint(Blueprint[NewConfig]):
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        )

        # Without force, should use cache
        registry.discover_blueprints()
        assert len(registry.list_blueprints()) == initial_count

        # With force, should rediscover
        registry.discover_blueprints(force=True)
        assert len(registry.list_blueprints()) == initial_count + 1

        names = [bp["name"] for bp in registry.list_blueprints()]
        assert "new_blueprint" in names

    def test_clear_registry(self, registry, temp_blueprints, monkeypatch):
        """Test clearing the registry."""
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])

        # Discover blueprints
        registry.discover_blueprints()
        assert len(registry.list_blueprints()) > 0

        # Clear
        registry.clear()

        # Should be empty until rediscovered
        assert registry._discovered is False
        assert len(registry._blueprints) == 0

        # Rediscover
        registry.discover_blueprints()
        assert len(registry.list_blueprints()) > 0

    def test_nested_templates(self, registry, temp_blueprints, monkeypatch):
        nested = temp_blueprints / "nested"
        nested.mkdir(parents=True)
        simple_bp = nested / "nested.py"
        simple_bp.write_text(
            """
from blueprint import Blueprint, BaseModel

class SimpleConfig(BaseModel):
    job_id: str

class NestedBlueprint(Blueprint[SimpleConfig]):
    '''A simple test blueprint.'''
    def render(self, config):
        from airflow import DAG
        from datetime import datetime
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        )
        monkeypatch.setattr(registry, "get_template_dirs", lambda: [temp_blueprints])
        registry.discover_blueprints(force=True)
        blueprints = registry.list_blueprints()
        assert "nested_blueprint" in [bp["name"] for bp in blueprints]
