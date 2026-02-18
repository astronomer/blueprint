"""Tests for the version-aware Blueprint registry."""

import pytest
from pydantic import BaseModel

from blueprint.core import Blueprint
from blueprint.errors import BlueprintNotFoundError, InvalidVersionError
from blueprint.registry import BlueprintRegistry


class SimpleConfig(BaseModel):
    name: str


class AdvancedConfig(BaseModel):
    items: list[str]


class Simple(Blueprint[SimpleConfig]):
    """A simple blueprint."""

    def render(self, config):
        pass


class SimpleV2(Blueprint[AdvancedConfig]):
    """Simple blueprint v2 with breaking changes."""

    def render(self, config):
        pass


class TestBlueprintRegistry:
    """Test the BlueprintRegistry functionality."""

    @pytest.fixture
    def reg(self):
        return BlueprintRegistry()

    @pytest.fixture
    def temp_blueprints(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir(parents=True)

        (template_dir / "blueprints.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class ExtractConfig(BaseModel):
    source: str

class Extract(Blueprint[ExtractConfig]):
    '''Extract data from a source.'''
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo extract")

class ExtractV2Config(BaseModel):
    sources: list[str]

class ExtractV2(Blueprint[ExtractV2Config]):
    '''Extract v2 with multi-source support.'''
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo extract_v2")

class LoadConfig(BaseModel):
    target: str

class Load(Blueprint[LoadConfig]):
    '''Load data to a target.'''
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo load")
""")

        return template_dir

    def test_discover_blueprints(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        blueprints = reg.list_blueprints()
        names = [bp["name"] for bp in blueprints]
        assert "extract" in names
        assert "load" in names

    def test_version_tracking(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        extract_info = next(bp for bp in reg.list_blueprints() if bp["name"] == "extract")
        assert sorted(extract_info["versions"]) == [1, 2]
        assert extract_info["latest_version"] == 2

    def test_get_latest_version(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        cls = reg.get("extract")
        assert cls.__name__ == "ExtractV2"

    def test_get_specific_version(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        cls_v1 = reg.get("extract", version=1)
        assert cls_v1.__name__ == "Extract"

        cls_v2 = reg.get("extract", version=2)
        assert cls_v2.__name__ == "ExtractV2"

    def test_get_nonexistent_name(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        with pytest.raises(BlueprintNotFoundError):
            reg.get("nonexistent")

    def test_get_nonexistent_version(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        with pytest.raises(InvalidVersionError):
            reg.get("extract", version=99)

    def test_get_blueprint_info(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        info = reg.get_blueprint_info("load")
        assert info["name"] == "load"
        assert info["class"] == "Load"
        assert info["version"] == 1
        assert "target" in info["parameters"]
        assert info["parameters"]["target"]["required"] is True

    def test_get_blueprint_info_versioned(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        info = reg.get_blueprint_info("extract", version=1)
        assert info["class"] == "Extract"
        assert "source" in info["parameters"]

        info_v2 = reg.get_blueprint_info("extract", version=2)
        assert info_v2["class"] == "ExtractV2"
        assert "sources" in info_v2["parameters"]

    def test_clear_and_rediscover(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        initial_count = len(reg.list_blueprints())
        assert initial_count > 0

        reg.clear()
        assert len(reg._blueprints) == 0
        assert reg._discovered is False

        reg.discover(force=True)
        assert len(reg.list_blueprints()) == initial_count

    def test_lazy_discovery(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])

        cls = reg.get("extract")
        assert cls is not None

    def test_no_force_uses_cache(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        initial_count = len(reg.list_blueprints())

        (temp_blueprints / "new_bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class NewConfig(BaseModel):
    x: int = 1

class NewBp(Blueprint[NewConfig]):
    def render(self, config):
        pass
""")

        reg.discover()
        assert len(reg.list_blueprints()) == initial_count

    def test_duplicate_blueprint_raises(self, tmp_path):
        from blueprint.errors import DuplicateBlueprintError

        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "aaa_first.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class DupConfig(BaseModel):
    x: int = 1

class Dup(Blueprint[DupConfig]):
    def render(self, config):
        pass
""")

        (template_dir / "zzz_second.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class DupConfig2(BaseModel):
    y: str = "hi"

class Dup(Blueprint[DupConfig2]):
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir])
        with pytest.raises(DuplicateBlueprintError, match="dup"):
            reg.discover(force=True)

    def test_template_dirs_constructor(self, temp_blueprints):
        reg = BlueprintRegistry(template_dirs=[temp_blueprints])
        reg.discover(force=True)

        blueprints = reg.list_blueprints()
        names = [bp["name"] for bp in blueprints]
        assert "extract" in names
        assert "load" in names

    def test_template_dirs_constructor_overrides_defaults(self, temp_blueprints):
        reg = BlueprintRegistry(template_dirs=[temp_blueprints])
        dirs = reg.get_template_dirs()
        assert dirs == [temp_blueprints]

    def test_default_template_dirs_no_duplicates(self, tmp_path, monkeypatch):
        dags_dir = tmp_path / "dags"
        dags_dir.mkdir()

        monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
        monkeypatch.chdir(tmp_path)

        reg = BlueprintRegistry()
        dirs = reg.get_template_dirs()
        resolved = [d.resolve() for d in dirs]
        assert len(resolved) == len(set(resolved)), f"Duplicate dirs found: {dirs}"

    def test_get_all_versions_info_single(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        versions = reg.get_all_versions_info("load")
        assert len(versions) == 1
        assert versions[0]["version"] == 1
        assert versions[0]["class"] == "Load"
        assert versions[0]["base_name"] == "Load"
        assert "properties" in versions[0]["schema"]
        assert "$defs" not in versions[0]["schema"]

    def test_get_all_versions_info_multi(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        versions = reg.get_all_versions_info("extract")
        assert len(versions) == 2
        assert versions[0]["version"] == 1
        assert versions[0]["class"] == "Extract"
        assert versions[0]["base_name"] == "Extract"
        assert versions[1]["version"] == 2
        assert versions[1]["class"] == "ExtractV2"
        assert versions[1]["base_name"] == "Extract"

    def test_get_all_versions_info_not_found(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        with pytest.raises(BlueprintNotFoundError):
            reg.get_all_versions_info("nonexistent")
