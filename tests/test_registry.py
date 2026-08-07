"""Tests for the version-aware Blueprint registry."""

import importlib
import importlib.metadata
import logging
from pathlib import Path

import pytest
from pydantic import BaseModel

from blueprint.core import Blueprint, DefaultDagArgs
from blueprint.errors import (
    BlueprintNotFoundError,
    DuplicateBlueprintError,
    EntryPointLoadError,
    InvalidVersionError,
    MultipleDagArgsError,
    NonContiguousVersionError,
)
from blueprint.registry import BlueprintRegistry, _defines_blueprint_subclass


def _blueprint_source(class_name: str, config_name: str = "Config") -> str:
    """Generate minimal Blueprint subclass source for entry-point discovery tests."""
    return f"""
from pydantic import BaseModel
from blueprint.core import Blueprint

class {config_name}(BaseModel):
    x: int = 1

class {class_name}(Blueprint[{config_name}]):
    def render(self, config):
        pass
"""


class _FakeEntryPoint:
    """
    Simulate installed entry points without having to package and installing distributions.
    """

    def __init__(self, name, value, dist_name=None, load_fn=None):
        self.name = name
        self.value = value
        self._dist_name = dist_name
        self._load_fn = load_fn or (lambda: importlib.import_module(value))

    def load(self):
        return self._load_fn()

    @property
    def dist(self):
        if self._dist_name is None:
            return None
        from types import SimpleNamespace

        return SimpleNamespace(name=self._dist_name)


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
        # Hermetic: directory-scan tests shouldn't pick up whatever this dev
        # venv happens to have installed under the entry-point group.
        return BlueprintRegistry(discover_entry_points=False)

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

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        with pytest.raises(DuplicateBlueprintError, match="dup"):
            reg.discover(force=True)

    def test_template_dirs_constructor(self, temp_blueprints):
        reg = BlueprintRegistry(template_dirs=[temp_blueprints], discover_entry_points=False)
        reg.discover(force=True)

        blueprints = reg.list_blueprints()
        names = [bp["name"] for bp in blueprints]
        assert "extract" in names
        assert "load" in names

    def test_template_dirs_constructor_overrides_defaults(self, temp_blueprints):
        reg = BlueprintRegistry(template_dirs=[temp_blueprints], discover_entry_points=False)
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
        assert "properties" in versions[0]["schema"]
        assert "$defs" not in versions[0]["schema"]

    def test_get_all_versions_info_multi(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        versions = reg.get_all_versions_info("extract")
        assert len(versions) == 2
        assert versions[0]["version"] == 1
        assert versions[0]["class"] == "Extract"
        assert versions[1]["version"] == 2
        assert versions[1]["class"] == "ExtractV2"

    def test_get_all_versions_info_not_found(self, reg, temp_blueprints, monkeypatch):
        monkeypatch.setattr(reg, "get_template_dirs", lambda: [temp_blueprints])
        reg.discover(force=True)

        with pytest.raises(BlueprintNotFoundError):
            reg.get_all_versions_info("nonexistent")

    def test_discover_explicit_name_blueprint(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "custom.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class MyConfig(BaseModel):
    x: int = 1

class MyCustomExtractor(Blueprint[MyConfig]):
    name = "extract"
    version = 1
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        cls = reg.get("extract")
        assert cls.__name__ == "MyCustomExtractor"

    def test_explicit_name_duplicate_detection(self, tmp_path):
        from blueprint.errors import DuplicateBlueprintError

        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "aaa_first.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class Cfg1(BaseModel):
    x: int = 1

class Extract(Blueprint[Cfg1]):
    def render(self, config):
        pass
""")

        (template_dir / "zzz_second.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class Cfg2(BaseModel):
    y: str = "hi"

class CustomExtractor(Blueprint[Cfg2]):
    name = "extract"
    version = 1
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        with pytest.raises(DuplicateBlueprintError, match="extract"):
            reg.discover(force=True)

    def test_non_contiguous_versions_raises(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "blueprints.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class Cfg1(BaseModel):
    x: int = 1

class Extract(Blueprint[Cfg1]):
    def render(self, config):
        pass

class Cfg3(BaseModel):
    z: str = "hi"

class ExtractV3(Blueprint[Cfg3]):
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        with pytest.raises(NonContiguousVersionError, match="Missing versions: 2"):
            reg.discover(force=True)

    def test_versions_not_starting_at_one_raises(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "blueprints.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class Cfg(BaseModel):
    x: int = 1

class MyExtractor(Blueprint[Cfg]):
    name = "extract"
    version = 2
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        with pytest.raises(NonContiguousVersionError, match="extract"):
            reg.discover(force=True)

    def test_get_all_versions_info_explicit_name(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "blueprints.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class Cfg1(BaseModel):
    x: int = 1

class MyExtractorV1(Blueprint[Cfg1]):
    name = "extract"
    version = 1
    def render(self, config):
        pass

class Cfg2(BaseModel):
    y: str = "hi"

class MyExtractorV2(Blueprint[Cfg2]):
    name = "extract"
    version = 2
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        versions = reg.get_all_versions_info("extract")
        assert len(versions) == 2
        assert versions[0]["class"] == "MyExtractorV1"
        assert versions[1]["class"] == "MyExtractorV2"


class TestDagArgsDiscovery:
    def test_no_dag_args_returns_default(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class XConfig(BaseModel):
    x: str = "a"

class X(Blueprint[XConfig]):
    def render(self, config):
        pass
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)
        assert reg.get_dag_args() is DefaultDagArgs

    def test_custom_dag_args_discovered(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "dag_args.py").write_text("""
from typing import Any
from pydantic import BaseModel, ConfigDict
from blueprint.core import BlueprintDagArgs

class MyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schedule: str | None = None

class MyDagArgs(BlueprintDagArgs[MyConfig]):
    def render(self, config) -> dict[str, Any]:
        return {"schedule": config.schedule} if config.schedule else {}
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        dag_args_cls = reg.get_dag_args()
        assert dag_args_cls is not DefaultDagArgs
        assert dag_args_cls.__name__ == "MyDagArgs"

    def test_multiple_dag_args_raises(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "aaa_first.py").write_text("""
from typing import Any
from pydantic import BaseModel
from blueprint.core import BlueprintDagArgs

class Config1(BaseModel):
    x: str = "a"

class DagArgs1(BlueprintDagArgs[Config1]):
    def render(self, config) -> dict[str, Any]:
        return {}
""")

        (template_dir / "zzz_second.py").write_text("""
from typing import Any
from pydantic import BaseModel
from blueprint.core import BlueprintDagArgs

class Config2(BaseModel):
    y: str = "b"

class DagArgs2(BlueprintDagArgs[Config2]):
    def render(self, config) -> dict[str, Any]:
        return {}
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        with pytest.raises(MultipleDagArgsError):
            reg.discover(force=True)

    def test_clear_resets_dag_args(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "dag_args.py").write_text("""
from typing import Any
from pydantic import BaseModel
from blueprint.core import BlueprintDagArgs

class ClearConfig(BaseModel):
    x: str = "a"

class ClearDagArgs(BlueprintDagArgs[ClearConfig]):
    def render(self, config) -> dict[str, Any]:
        return {}
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)
        assert reg.get_dag_args() is not DefaultDagArgs

        reg.clear()
        assert reg._dag_args is None

    def test_dag_args_not_in_list_blueprints(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "all.py").write_text("""
from typing import Any
from pydantic import BaseModel
from blueprint.core import Blueprint, BlueprintDagArgs

class BpConfig(BaseModel):
    x: str = "a"

class MyBp(Blueprint[BpConfig]):
    def render(self, config):
        pass

class DaConfig(BaseModel):
    y: str = "b"

class MyDa(BlueprintDagArgs[DaConfig]):
    def render(self, config) -> dict[str, Any]:
        return {}
""")

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        bp_names = [bp["name"] for bp in reg.list_blueprints()]
        assert "my_bp" in bp_names
        assert "my_da" not in bp_names


class TestNonBlueprintFileFiltering:
    """Verify that non-blueprint .py files in the search path are not executed.

    The registry previously exec'd every .py file to find Blueprint subclasses.
    That meant Python DAG files, utility modules, and any other code that
    happened to live alongside blueprint definitions had their top-level code
    run as a side effect of discovery (e.g. creating DAG objects, opening
    network sockets, etc.). The AST-based pre-filter avoids that.
    """

    def test_defines_blueprint_subclass_recognizes_subscripted_base(self, tmp_path):
        py_file = tmp_path / "bp.py"
        py_file.write_text(
            "from blueprint.core import Blueprint\nclass X(Blueprint[object]):\n    pass\n"
        )
        assert _defines_blueprint_subclass(py_file) is True

    def test_defines_blueprint_subclass_recognizes_bare_base(self, tmp_path):
        py_file = tmp_path / "bp.py"
        py_file.write_text("from blueprint.core import Blueprint\nclass X(Blueprint):\n    pass\n")
        assert _defines_blueprint_subclass(py_file) is True

    def test_defines_blueprint_subclass_recognizes_blueprint_dag_args(self, tmp_path):
        py_file = tmp_path / "da.py"
        py_file.write_text(
            "from blueprint.core import BlueprintDagArgs\n"
            "class X(BlueprintDagArgs[object]):\n"
            "    pass\n"
        )
        assert _defines_blueprint_subclass(py_file) is True

    def test_defines_blueprint_subclass_rejects_plain_module(self, tmp_path):
        py_file = tmp_path / "utils.py"
        py_file.write_text("def helper():\n    return 42\n")
        assert _defines_blueprint_subclass(py_file) is False

    def test_defines_blueprint_subclass_rejects_unrelated_class(self, tmp_path):
        py_file = tmp_path / "other.py"
        py_file.write_text("class Foo:\n    pass\nclass Bar(Foo):\n    pass\n")
        assert _defines_blueprint_subclass(py_file) is False

    def test_defines_blueprint_subclass_rejects_syntax_error(self, tmp_path):
        py_file = tmp_path / "broken.py"
        py_file.write_text("def (\n")
        assert _defines_blueprint_subclass(py_file) is False

    def test_defines_blueprint_subclass_misses_aliased_import(self, tmp_path):
        """Documented limitation: aliased imports aren't matched by the AST scan."""
        py_file = tmp_path / "aliased.py"
        py_file.write_text(
            "from blueprint.core import Blueprint as B\nclass X(B[object]):\n    pass\n"
        )
        assert _defines_blueprint_subclass(py_file) is False

    def test_non_blueprint_file_is_not_executed(self, tmp_path):
        """A side-effecting non-blueprint file is left alone during discovery.

        If the registry exec'd this file, the sentinel marker file would appear.
        The blueprint registry must not run user Python that does not declare
        a Blueprint subclass.
        """
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        sentinel = tmp_path / "side_effect_ran"
        side_effect_file = template_dir / "hybrid_dag.py"
        side_effect_file.write_text(
            f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('triggered')\n"
        )

        # Provide a real blueprint so discovery has something to find.
        (template_dir / "blueprints.py").write_text(
            "from pydantic import BaseModel\n"
            "from blueprint.core import Blueprint\n"
            "class Cfg(BaseModel):\n"
            "    x: str = 'a'\n"
            "class Real(Blueprint[Cfg]):\n"
            "    def render(self, config):\n"
            "        pass\n"
        )

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        bp_names = [bp["name"] for bp in reg.list_blueprints()]
        assert "real" in bp_names, "Blueprint discovery should still find real blueprints"
        assert not sentinel.exists(), (
            "Registry exec'd a non-blueprint .py file — the AST pre-filter is not active"
        )

    def test_blueprint_file_is_still_executed(self, tmp_path):
        """Files that declare a Blueprint subclass must still be imported and registered."""
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        (template_dir / "etl.py").write_text(
            "from pydantic import BaseModel\n"
            "from blueprint.core import Blueprint\n"
            "class Cfg(BaseModel):\n"
            "    x: str = 'a'\n"
            "class Etl(Blueprint[Cfg]):\n"
            "    def render(self, config):\n"
            "        pass\n"
        )

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        bp_names = [bp["name"] for bp in reg.list_blueprints()]
        assert "etl" in bp_names


class TestBlueprintLocations:
    """Test how the registry tracks and reports blueprint source locations."""

    def test_list_blueprints_reports_absolute_location(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "etl.py").write_text(
            "from pydantic import BaseModel\n"
            "from blueprint.core import Blueprint\n"
            "class Cfg(BaseModel):\n"
            "    x: str = 'a'\n"
            "class Etl(Blueprint[Cfg]):\n"
            "    def render(self, config):\n"
            "        pass\n"
        )

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=False)
        reg.discover(force=True)

        location = reg.list_blueprints()[0]["locations"][1]
        assert Path(location).is_absolute()
        assert Path(location) == (template_dir / "etl.py").resolve()


class TestEntryPointDiscovery:
    """Test discovering blueprints from installed packages via entry points."""

    def _patch_entry_points(self, monkeypatch, eps):
        monkeypatch.setattr(importlib.metadata, "entry_points", lambda **_: eps)

    def test_entry_point_discovery_basic(self, tmp_path, monkeypatch):
        """Checks that a single installed entry-point module is discovered as a blueprint."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_basic_mod.py").write_text(_blueprint_source("BasicBp"))
        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("basic", "_ep_basic_mod")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        reg.discover(force=True)

        blueprints = reg.list_blueprints()
        assert [bp["name"] for bp in blueprints] == ["basic_bp"]
        assert blueprints[0]["locations"][1] == "_ep_basic_mod"

    def test_entry_point_discovery_recursive_package(self, tmp_path, monkeypatch):
        """Checks that when an entry point targets a package, its blueprint submodules are found too."""
        monkeypatch.syspath_prepend(str(tmp_path))
        pkg_dir = tmp_path / "_ep_recursive_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("from .primary import Primary  # noqa: F401\n")
        (pkg_dir / "primary.py").write_text(_blueprint_source("Primary", "PrimaryConfig"))
        (pkg_dir / "secondary.py").write_text(_blueprint_source("Secondary", "SecondaryConfig"))

        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("recursive", "_ep_recursive_pkg")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        reg.discover(force=True)  # would raise DuplicateBlueprintError if double-registered

        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"primary", "secondary"}

    def test_entry_point_discovery_multiple_entry_points(self, tmp_path, monkeypatch):
        """Checks that blueprints from more than one installed entry point are all discovered."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_multi_a.py").write_text(_blueprint_source("MultiA"))
        (tmp_path / "_ep_multi_b.py").write_text(_blueprint_source("MultiB"))

        self._patch_entry_points(
            monkeypatch,
            [_FakeEntryPoint("a", "_ep_multi_a"), _FakeEntryPoint("b", "_ep_multi_b")],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        reg.discover(force=True)

        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"multi_a", "multi_b"}

    def test_entry_point_duplicate_with_local_directory_raises(self, tmp_path, monkeypatch):
        """Checks that discovery fails when the same blueprint exists both locally and in an installed package."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_dup_local.py").write_text(_blueprint_source("DupLocal"))
        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("local", "_ep_dup_local")])

        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "blueprints.py").write_text(_blueprint_source("DupLocal"))

        reg = BlueprintRegistry(template_dirs=[template_dir], discover_entry_points=True)
        with pytest.raises(DuplicateBlueprintError, match="dup_local"):
            reg.discover(force=True)

    def test_entry_point_duplicate_across_two_entry_points_raises(self, tmp_path, monkeypatch):
        """Checks that discovery fails when two installed packages export the same blueprint name and version."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_dup_a.py").write_text(_blueprint_source("DupSame", "DupSameConfigA"))
        (tmp_path / "_ep_dup_b.py").write_text(_blueprint_source("DupSame", "DupSameConfigB"))

        self._patch_entry_points(
            monkeypatch,
            [_FakeEntryPoint("a", "_ep_dup_a"), _FakeEntryPoint("b", "_ep_dup_b")],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        with pytest.raises(DuplicateBlueprintError, match="dup_same"):
            reg.discover(force=True)

    def test_entry_point_broken_module_raises(self, tmp_path, monkeypatch):
        """Checks that an entry point that fails to load surfaces the underlying import error."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_good.py").write_text(_blueprint_source("GoodBp"))

        def _raise():
            msg = "boom"
            raise ImportError(msg)

        self._patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("broken", "_ep_missing", load_fn=_raise),
                _FakeEntryPoint("good", "_ep_good"),
            ],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        with pytest.raises(EntryPointLoadError, match="ImportError: boom") as exc_info:
            reg.discover(force=True)

        assert exc_info.value.entry_point == "broken"
        assert isinstance(exc_info.value.__cause__, ImportError)

    def test_entry_point_broken_module_skipped_when_skip_invalid(
        self, tmp_path, monkeypatch, caplog
    ):
        """Checks that with skip_invalid one broken entry point does not prevent good packages from loading."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_good_skip.py").write_text(_blueprint_source("GoodSkipBp"))

        def _raise():
            msg = "boom"
            raise ImportError(msg)

        self._patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("broken", "_ep_missing", load_fn=_raise),
                _FakeEntryPoint("good", "_ep_good_skip"),
            ],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True, skip_invalid=True)
        with caplog.at_level(logging.WARNING):
            reg.discover(force=True)

        assert any("broken" in rec.getMessage() for rec in caplog.records)
        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"good_skip_bp"}

    def test_entry_point_non_module_target_raises(self, tmp_path, monkeypatch):
        """Checks that an entry point pointing to the wrong kind of object fails with a clear error."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_good2.py").write_text(_blueprint_source("GoodBp2"))

        self._patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("bad", "_ep_bad:attr", load_fn=lambda: 42),
                _FakeEntryPoint("good", "_ep_good2"),
            ],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        with pytest.raises(EntryPointLoadError, match="does not resolve to a module"):
            reg.discover(force=True)

    def test_entry_point_non_module_target_skipped_when_skip_invalid(
        self, tmp_path, monkeypatch, caplog
    ):
        """Checks that with skip_invalid an entry point resolving to a non-module is ignored, not fatal."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_good2_skip.py").write_text(_blueprint_source("GoodBp2Skip"))

        self._patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("bad", "_ep_bad:attr", load_fn=lambda: 42),
                _FakeEntryPoint("good", "_ep_good2_skip"),
            ],
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True, skip_invalid=True)
        with caplog.at_level(logging.WARNING):
            reg.discover(force=True)

        assert any("does not resolve to a module" in rec.getMessage() for rec in caplog.records)
        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"good_bp2_skip"}

    def test_entry_point_broken_leaf_submodule_raises(self, tmp_path, monkeypatch):
        """Checks that a bad module inside an entry-point package fails discovery with its import error."""
        monkeypatch.syspath_prepend(str(tmp_path))
        pkg_dir = tmp_path / "_ep_broken_leaf_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")
        (pkg_dir / "broken.py").write_text("raise ImportError('leaf boom')\n")
        (pkg_dir / "fine.py").write_text(_blueprint_source("FineBp"))

        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("leaf", "_ep_broken_leaf_pkg")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        with pytest.raises(EntryPointLoadError, match="_ep_broken_leaf_pkg.broken"):
            reg.discover(force=True)

    def test_entry_point_broken_leaf_submodule_skipped_when_skip_invalid(
        self, tmp_path, monkeypatch, caplog
    ):
        """Checks that with skip_invalid one bad module does not stop its siblings from being discovered."""
        monkeypatch.syspath_prepend(str(tmp_path))
        pkg_dir = tmp_path / "_ep_broken_leaf_skip_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")
        (pkg_dir / "broken.py").write_text("raise ImportError('leaf boom')\n")
        (pkg_dir / "fine.py").write_text(_blueprint_source("FineSkipBp"))

        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("leaf", "_ep_broken_leaf_skip_pkg")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True, skip_invalid=True)
        with caplog.at_level(logging.WARNING):
            reg.discover(force=True)

        assert any("broken" in rec.getMessage() for rec in caplog.records)
        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"fine_skip_bp"}

    def test_entry_point_broken_subpackage_raises(self, tmp_path, monkeypatch):
        """Checks that a broken subpackage of an entry-point package fails discovery."""
        monkeypatch.syspath_prepend(str(tmp_path))
        pkg_dir = tmp_path / "_ep_broken_subpkg_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        broken_sub = pkg_dir / "broken_sub"
        broken_sub.mkdir()
        (broken_sub / "__init__.py").write_text("raise RuntimeError('subpackage boom')\n")

        fine_sub = pkg_dir / "fine_sub"
        fine_sub.mkdir()
        (fine_sub / "__init__.py").write_text("")
        (fine_sub / "bp.py").write_text(_blueprint_source("FineSub"))

        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("subpkg", "_ep_broken_subpkg_pkg")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        with pytest.raises(EntryPointLoadError, match="_ep_broken_subpkg_pkg.broken_sub"):
            reg.discover(force=True)

    def test_entry_point_broken_subpackage_skipped_when_skip_invalid(
        self, tmp_path, monkeypatch, caplog
    ):
        """Checks that with skip_invalid a broken subpackage does not stop discovery of its siblings."""
        monkeypatch.syspath_prepend(str(tmp_path))
        pkg_dir = tmp_path / "_ep_broken_subpkg_skip_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        broken_sub = pkg_dir / "broken_sub"
        broken_sub.mkdir()
        (broken_sub / "__init__.py").write_text("raise RuntimeError('subpackage boom')\n")

        fine_sub = pkg_dir / "fine_sub"
        fine_sub.mkdir()
        (fine_sub / "__init__.py").write_text("")
        (fine_sub / "bp.py").write_text(_blueprint_source("FineSubSkip"))

        self._patch_entry_points(
            monkeypatch, [_FakeEntryPoint("subpkg", "_ep_broken_subpkg_skip_pkg")]
        )

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True, skip_invalid=True)
        with caplog.at_level(logging.WARNING):
            reg.discover(force=True)

        names = {bp["name"] for bp in reg.list_blueprints()}
        assert names == {"fine_sub_skip"}

    def test_discover_entry_points_false_disables_discovery(self, tmp_path, monkeypatch):
        """Checks that entry-point discovery is completely skipped when the feature is turned off."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_disabled_mod.py").write_text(_blueprint_source("DisabledBp"))

        calls = []

        def _fake_entry_points(**kwargs):
            calls.append(kwargs)
            return [_FakeEntryPoint("disabled", "_ep_disabled_mod")]

        monkeypatch.setattr(importlib.metadata, "entry_points", _fake_entry_points)

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=False)
        reg.discover(force=True)

        assert reg.list_blueprints() == []
        assert calls == []

    def test_entry_point_module_reused_across_force_rediscovery(self, tmp_path, monkeypatch):
        """Checks that rediscovering entry points reuses the already imported module instead of creating a new class object."""
        monkeypatch.syspath_prepend(str(tmp_path))
        (tmp_path / "_ep_cache_mod.py").write_text(_blueprint_source("CacheBp"))
        self._patch_entry_points(monkeypatch, [_FakeEntryPoint("cache", "_ep_cache_mod")])

        reg = BlueprintRegistry(template_dirs=[], discover_entry_points=True)
        reg.discover(force=True)
        cls_first = reg.get("cache_bp")

        reg.discover(force=True)
        cls_second = reg.get("cache_bp")

        assert cls_first is cls_second
