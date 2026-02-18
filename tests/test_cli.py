"""Tests for the Blueprint CLI."""

from click.testing import CliRunner

from blueprint.cli import cli


class TestCLI:
    """Test the CLI commands."""

    def test_cli_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Blueprint" in result.output

    def test_cli_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0

    def test_list_command_empty(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--template-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No blueprints found" in result.output

    def test_list_command_with_blueprints(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "test.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class TestConfig(BaseModel):
    x: int = 1

class TestBlueprint(Blueprint[TestConfig]):
    '''A test blueprint.'''
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "test_blueprint" in result.output

    def test_list_shows_versions(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class FooConfig(BaseModel):
    x: int = 1

class Foo(Blueprint[FooConfig]):
    '''Foo v1.'''
    def render(self, config):
        pass

class FooV2Config(BaseModel):
    y: str = "hello"

class FooV2(Blueprint[FooV2Config]):
    '''Foo v2.'''
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "foo" in result.output.lower()
        assert "1" in result.output
        assert "2" in result.output

    def test_describe_command(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel, Field
from blueprint.core import Blueprint

class DetailConfig(BaseModel):
    name: str = Field(description="The name")
    count: int = Field(default=5, description="Count value")

class Detail(Blueprint[DetailConfig]):
    '''A detailed blueprint for testing.'''
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["describe", "detail", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "Detail" in result.output
        assert "name" in result.output
        assert "count" in result.output

    def test_describe_with_version(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class FooConfig(BaseModel):
    x: int = 1

class Foo(Blueprint[FooConfig]):
    '''Foo v1.'''
    def render(self, config):
        pass

class FooV2Config(BaseModel):
    y: str

class FooV2(Blueprint[FooV2Config]):
    '''Foo v2.'''
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["describe", "foo", "-v", "1", "--template-dir", str(template_dir)]
        )
        assert result.exit_code == 0
        assert "Foo" in result.output
        assert "v1" in result.output

    def test_describe_nonexistent(self, tmp_path):
        template_dir = tmp_path / "empty"
        template_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            cli, ["describe", "nonexistent", "--template-dir", str(template_dir)]
        )
        assert result.exit_code == 1

    def test_lint_valid_yaml(self, tmp_path):
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

        runner = CliRunner()
        result = runner.invoke(cli, ["lint", str(yaml_file), "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "PASS" in result.output

    def test_lint_invalid_yaml(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()

        yaml_file = tmp_path / "bad.dag.yaml"
        yaml_file.write_text("dag_id: test\nsteps:\n  s:\n    blueprint: missing\n")

        runner = CliRunner()
        result = runner.invoke(cli, ["lint", str(yaml_file), "--template-dir", str(template_dir)])
        assert result.exit_code == 1
        assert "FAIL" in result.output

    def test_lint_no_files(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["lint"])
        assert result.exit_code == 0
        assert "No .dag.yaml files found" in result.output

    def test_lint_duplicate_dag_ids(self, tmp_path):
        from pathlib import Path

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

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path("first.dag.yaml").write_text(
                "dag_id: same_id\nsteps:\n  s1:\n    blueprint: stub\n"
            )
            Path("second.dag.yaml").write_text(
                "dag_id: same_id\nsteps:\n  s2:\n    blueprint: stub\n"
            )
            result = runner.invoke(cli, ["lint", "--template-dir", str(template_dir)])

        assert result.exit_code == 1
        assert "Duplicate DAG ID" in result.output

    def test_schema_command_single_version(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel, Field
from blueprint.core import Blueprint

class SchemaConfig(BaseModel):
    name: str = Field(description="The name")

class SchemaBlueprint(Blueprint[SchemaConfig]):
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["schema", "schema_blueprint", "--template-dir", str(template_dir)]
        )
        assert result.exit_code == 0
        assert "name" in result.output
        assert "schema_blueprint" in result.output
        assert "version" in result.output
        assert "$defs" not in result.output
        assert "$ref" not in result.output

    def test_schema_command_multi_version(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel, Field
from blueprint.core import Blueprint

class FooConfig(BaseModel):
    x: int = 1

class Foo(Blueprint[FooConfig]):
    def render(self, config):
        pass

class FooV2Config(BaseModel):
    y: str
    z: bool = True

class FooV2(Blueprint[FooV2Config]):
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["schema", "foo", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "oneOf" in result.output
        assert '"discriminator"' in result.output
        assert '"propertyName": "version"' in result.output
        assert "$defs" not in result.output
        assert "$ref" not in result.output

    def test_schema_command_version_required(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class BarConfig(BaseModel):
    val: str

class Bar(Blueprint[BarConfig]):
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["schema", "bar", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert '"version"' in result.output
        assert '"blueprint"' in result.output

    def test_schema_command_base_name_title(self, tmp_path):
        template_dir = tmp_path / "dags"
        template_dir.mkdir()
        (template_dir / "bp.py").write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class ExtConfig(BaseModel):
    a: str

class Ext(Blueprint[ExtConfig]):
    def render(self, config):
        pass

class ExtV2Config(BaseModel):
    b: int = 1

class ExtV2(Blueprint[ExtV2Config]):
    def render(self, config):
        pass
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["schema", "ext", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "oneOf" in result.output
        output_lines = result.output.strip()
        assert '"Ext"' in output_lines
        assert '"ExtV2"' not in output_lines
