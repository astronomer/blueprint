"""Tests for YAML loading, Jinja2 rendering, and blueprint discovery."""

import pytest

from blueprint.core import Blueprint
from blueprint.errors import (
    BlueprintNotFoundError,
    ConfigurationError,
    CyclicDependencyError,
    InvalidDependencyError,
)
from blueprint.loaders import (
    discover_blueprints,
    get_blueprint_info,
    load_blueprint,
    render_yaml_template,
    validate_yaml,
)


class TestRenderYamlTemplate:
    """Test YAML template loading and Jinja2 rendering."""

    def test_basic_yaml_load(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("dag_id: test\nschedule: '@daily'\nsteps:\n  s: {blueprint: x}\n")

        config, rendered = render_yaml_template(yaml_file, use_airflow_context=False)
        assert config["dag_id"] == "test"
        assert config["schedule"] == "@daily"

    def test_jinja2_rendering(self, tmp_path):
        yaml_file = tmp_path / "jinja.yaml"
        yaml_file.write_text("dag_id: {{ prefix }}_pipeline\nsteps:\n  s: {blueprint: x}\n")

        config, rendered = render_yaml_template(
            yaml_file, context={"prefix": "prod"}, use_airflow_context=False
        )
        assert config["dag_id"] == "prod_pipeline"

    def test_empty_yaml_raises(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        with pytest.raises(ConfigurationError, match="empty or invalid"):
            render_yaml_template(yaml_file, use_airflow_context=False)

    def test_invalid_yaml_raises(self, tmp_path):
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("dag_id: test\n  bad_indent: true\n")

        with pytest.raises(ConfigurationError):
            render_yaml_template(yaml_file, use_airflow_context=False)

    def test_missing_file_raises(self, tmp_path):
        yaml_file = tmp_path / "missing.yaml"

        with pytest.raises(ConfigurationError, match="Failed to read"):
            render_yaml_template(yaml_file, use_airflow_context=False)

    def test_jinja_render_error_raises_configuration_error(self, tmp_path):
        yaml_file = tmp_path / "bad_jinja.yaml"
        yaml_file.write_text("dag_id: {{ undefined_var }}\nsteps:\n  s: {blueprint: x}\n")

        with pytest.raises(ConfigurationError, match="Jinja2 template rendering failed"):
            render_yaml_template(yaml_file, use_airflow_context=False)

    def test_jinja_strict_undefined(self, tmp_path):
        yaml_file = tmp_path / "strict.yaml"
        yaml_file.write_text("dag_id: {{ missing }}\nsteps:\n  s: {blueprint: x}\n")

        with pytest.raises(ConfigurationError):
            render_yaml_template(yaml_file, context={}, use_airflow_context=False)


class TestLoadBlueprint:
    """Test loading blueprints by name."""

    def test_load_from_template_dir(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class MyConfig(BaseModel):
    x: int = 1

class MyBlueprint(Blueprint[MyConfig]):
    '''Test blueprint.'''
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        cls = load_blueprint("my_blueprint", template_dir=str(template_dir))
        assert cls.__name__ == "MyBlueprint"
        assert issubclass(cls, Blueprint)

    def test_load_versioned(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class FooConfig(BaseModel):
    x: int = 1

class Foo(Blueprint[FooConfig]):
    def render(self, config):
        pass

class FooV2Config(BaseModel):
    y: str = "hello"

class FooV2(Blueprint[FooV2Config]):
    def render(self, config):
        pass
""")

        cls_latest = load_blueprint("foo", template_dir=str(template_dir))
        assert cls_latest.__name__ == "FooV2"

        cls_v1 = load_blueprint("foo", template_dir=str(template_dir), version=1)
        assert cls_v1.__name__ == "Foo"

    def test_load_not_found(self, tmp_path):
        template_dir = tmp_path / "empty"
        template_dir.mkdir()

        with pytest.raises(BlueprintNotFoundError):
            load_blueprint("nonexistent", template_dir=str(template_dir))


class TestDiscoverBlueprints:
    """Test blueprint discovery."""

    def test_discover_in_directory(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bps.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class AConfig(BaseModel):
    x: int = 1

class AlphaBlueprint(Blueprint[AConfig]):
    '''Alpha blueprint.'''
    def render(self, config):
        pass

class BConfig(BaseModel):
    y: str = "hello"

class BetaBlueprint(Blueprint[BConfig]):
    '''Beta blueprint.'''
    def render(self, config):
        pass
""")

        blueprints = discover_blueprints(str(template_dir))
        names = [bp["name"] for bp in blueprints]
        assert "alpha_blueprint" in names
        assert "beta_blueprint" in names

    def test_discover_empty_dir(self, tmp_path):
        template_dir = tmp_path / "empty"
        template_dir.mkdir()
        assert discover_blueprints(str(template_dir)) == []


class TestGetBlueprintInfo:
    """Test getting detailed blueprint information."""

    def test_get_info(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel, Field
from blueprint.core import Blueprint

class DetailConfig(BaseModel):
    name: str = Field(description="The name")
    count: int = Field(default=5, ge=1, description="Count value")

class Detail(Blueprint[DetailConfig]):
    '''A detailed blueprint.'''
    def render(self, config):
        pass
""")

        info = get_blueprint_info("detail", str(template_dir))
        assert info["name"] == "detail"
        assert info["class"] == "Detail"
        assert "name" in info["parameters"]
        assert info["parameters"]["name"]["required"] is True
        assert info["parameters"]["count"]["default"] == 5


class TestValidateYaml:
    """Test YAML validation without building a DAG."""

    def test_valid_yaml(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        pass
""")

        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text("dag_id: test\nsteps:\n  s:\n    blueprint: stub\n")

        result = validate_yaml(str(yaml_file), template_dir=str(template_dir))
        assert result["dag_id"] == "test"

    def test_invalid_blueprint_name(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        yaml_file = tmp_path / "bad.dag.yaml"
        yaml_file.write_text("dag_id: test\nsteps:\n  s:\n    blueprint: nonexistent\n")

        with pytest.raises(BlueprintNotFoundError):
            validate_yaml(str(yaml_file), template_dir=str(template_dir))

    def test_invalid_step_config(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel, Field
from blueprint.core import Blueprint

class ReqConfig(BaseModel):
    required_field: str

class Req(Blueprint[ReqConfig]):
    def render(self, config):
        pass
""")

        yaml_file = tmp_path / "bad.dag.yaml"
        yaml_file.write_text("dag_id: test\nsteps:\n  s:\n    blueprint: req\n")

        with pytest.raises((ConfigurationError, ValueError)):
            validate_yaml(str(yaml_file), template_dir=str(template_dir))

    def test_invalid_dependency_reference(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        pass
""")

        yaml_file = tmp_path / "bad_dep.dag.yaml"
        yaml_file.write_text(
            "dag_id: test\nsteps:\n  step_a:\n    blueprint: stub\n    depends_on: [nonexistent]\n"
        )

        with pytest.raises(InvalidDependencyError, match="nonexistent"):
            validate_yaml(str(yaml_file), template_dir=str(template_dir))

    def test_cyclic_dependency(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        pass
""")

        yaml_file = tmp_path / "cycle.dag.yaml"
        yaml_file.write_text(
            "dag_id: test\nsteps:\n"
            "  a:\n    blueprint: stub\n    depends_on: [b]\n"
            "  b:\n    blueprint: stub\n    depends_on: [a]\n"
        )

        with pytest.raises(CyclicDependencyError):
            validate_yaml(str(yaml_file), template_dir=str(template_dir))
