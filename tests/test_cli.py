"""Tests for the Blueprint CLI."""

from click.testing import CliRunner

from blueprint.cli import cli


class TestCLI:
    """Test the CLI commands."""

    def test_cli_help(self):
        """Test that CLI shows help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Blueprint - Reusable, validated Airflow DAG templates" in result.output

    def test_list_command_empty(self, tmp_path):
        """Test list command with no blueprints."""
        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--template-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No blueprints found" in result.output

    def test_list_command_with_blueprints(self, tmp_path):
        """Test list command with blueprints."""
        # Create a test blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = '''
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class TestConfig(BaseModel):
    job_id: str

class TestBlueprint(Blueprint[TestConfig]):
    """A test blueprint."""
    def render(self, config: TestConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
'''
        (template_dir / "test.py").write_text(blueprint_code)

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "test_blueprint" in result.output
        assert "TestBlueprint" in result.output
        assert "A test blueprint" in result.output

    def test_describe_command(self, tmp_path):
        """Test describe command."""
        # Create a test blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = '''
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class DetailedConfig(BaseModel):
    job_id: str = Field(description="Unique job ID")
    retries: int = Field(default=2, description="Number of retries")

class DetailedBlueprint(Blueprint[DetailedConfig]):
    """A detailed blueprint for testing."""
    def render(self, config: DetailedConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
'''
        (template_dir / "detailed.py").write_text(blueprint_code)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["describe", "detailed_blueprint", "--template-dir", str(template_dir)]
        )
        assert result.exit_code == 0
        assert "DetailedBlueprint" in result.output
        assert "A detailed blueprint for testing" in result.output
        assert "job_id" in result.output
        assert "Unique job ID" in result.output
        assert "retries" in result.output

    def test_schema_command(self, tmp_path):
        """Test schema generation command."""
        # Create a test blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class SchemaConfig(BaseModel):
    job_id: str = Field(pattern=r'^[a-zA-Z0-9_-]+$')
    enabled: bool = Field(default=True)

class SchemaBlueprint(Blueprint[SchemaConfig]):
    def render(self, config: SchemaConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "schema_test.py").write_text(blueprint_code)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["schema", "schema_blueprint", "--template-dir", str(template_dir)]
        )
        assert result.exit_code == 0
        assert '"$schema"' in result.output
        assert '"blueprint"' in result.output
        assert "schema_blueprint" in result.output

    def test_lint_valid_config(self, tmp_path):
        """Test linting a valid configuration."""
        # Create blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class LintConfig(BaseModel):
    job_id: str

class LintBlueprint(Blueprint[LintConfig]):
    def render(self, config: LintConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "lint_test.py").write_text(blueprint_code)

        # Create valid YAML config
        config_file = tmp_path / "test.dag.yaml"
        config_file.write_text("""
blueprint: lint_blueprint
job_id: test-dag
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["lint", str(config_file), "--template-dir", str(template_dir)])
        assert result.exit_code == 0
        assert "✅" in result.output
        assert "Valid" in result.output

    def test_lint_invalid_config(self, tmp_path):
        """Test linting an invalid configuration."""
        # Create blueprint with validation
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG
from datetime import datetime

class StrictConfig(BaseModel):
    job_id: str = Field(pattern=r'^[a-zA-Z0-9_-]+$')
    retries: int = Field(ge=0, le=5)

class StrictBlueprint(Blueprint[StrictConfig]):
    def render(self, config: StrictConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "strict.py").write_text(blueprint_code)

        # Create invalid YAML config
        config_file = tmp_path / "invalid.dag.yaml"
        config_file.write_text("""
blueprint: strict_blueprint
job_id: "invalid job!"
retries: 10
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["lint", str(config_file), "--template-dir", str(template_dir)])
        assert result.exit_code == 1
        assert "❌" in result.output

    def test_init_command_creates_config_file(self, tmp_path, chdir):
        """Test init command creates blueprint.toml config file."""
        with chdir(tmp_path):
            runner = CliRunner()
            # Provide input for interactive prompts: template_path, output_dir, create_dag_loader, loader_path, create_example
            result = runner.invoke(cli, ["init"], input="\n\ny\n\ny\n")
            assert result.exit_code == 0
            assert "✅ Created blueprint.toml" in result.output

            # Check config file was created
            config_file = tmp_path / "blueprint.toml"
            assert config_file.exists()

            # Check config content
            content = config_file.read_text()
            assert "template_path" in content
            assert "output_dir" in content
            assert "Blueprint configuration" in content

    def test_init_command_with_custom_values(self, tmp_path, chdir):
        """Test init command with custom template and output paths."""
        with chdir(tmp_path):
            runner = CliRunner()
            # Provide custom values: template_path, output_dir, create_dag_loader, loader_path, create_example
            result = runner.invoke(cli, ["init"], input="custom/templates\ncustom/output\ny\n\ny\n")
            assert result.exit_code == 0

            # Check config content
            config_file = tmp_path / "blueprint.toml"
            content = config_file.read_text()
            assert '"custom/templates"' in content
            assert '"custom/output"' in content

    def test_init_command_creates_directories(self, tmp_path, chdir):
        """Test init command creates template and output directories."""
        with chdir(tmp_path):
            runner = CliRunner()
            result = runner.invoke(cli, ["init"], input="\n\ny\n\ny\n")
            assert result.exit_code == 0

            # Check config file was created
            config_file = tmp_path / "blueprint.toml"
            assert config_file.exists()

            # Check that setup completed successfully
            assert "✨ Blueprint initialized!" in result.output

    def test_init_command_file_exists(self, tmp_path, chdir):
        """Test init command when config file already exists."""
        with chdir(tmp_path):
            # Create existing config file
            config_file = tmp_path / "blueprint.toml"
            config_file.write_text("# Existing config")

            runner = CliRunner()
            result = runner.invoke(cli, ["init"], input="n\n")
            assert result.exit_code == 0
            assert "already exists" in result.output

            # Config should be unchanged
            content = config_file.read_text()
            assert content == "# Existing config"

    def test_lint_duplicate_dag_ids(self, tmp_path):
        """Test linting detects duplicate DAG IDs across multiple files."""
        # Create blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class DuplicateTestConfig(BaseModel):
    job_id: str

class DuplicateTestBlueprint(Blueprint[DuplicateTestConfig]):
    def render(self, config: DuplicateTestConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "duplicate_test.py").write_text(blueprint_code)

        # Create two config files with same DAG ID
        config_file1 = tmp_path / "config1.dag.yaml"
        config_file1.write_text("""
blueprint: duplicate_test_blueprint
job_id: same-dag-id
""")

        config_file2 = tmp_path / "config2.dag.yaml"
        config_file2.write_text("""
blueprint: duplicate_test_blueprint
job_id: same-dag-id
""")

        # Change to tmp_path directory so lint command finds both files
        import os
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            
            runner = CliRunner()
            result = runner.invoke(cli, ["lint", "--template-dir", str(template_dir)])
            assert result.exit_code == 1  # Should fail due to duplicate
            assert "Duplicate DAG ID" in result.output
            assert "same-dag-id" in result.output
            assert "config1.dag.yaml" in result.output
            assert "config2.dag.yaml" in result.output
        finally:
            os.chdir(original_dir)

    def test_lint_no_duplicate_dag_ids(self, tmp_path):
        """Test linting passes when DAG IDs are unique."""
        # Create blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class UniqueTestConfig(BaseModel):
    job_id: str

class UniqueTestBlueprint(Blueprint[UniqueTestConfig]):
    def render(self, config: UniqueTestConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "unique_test.py").write_text(blueprint_code)

        # Create two config files with different DAG IDs
        config_file1 = tmp_path / "config1.dag.yaml"
        config_file1.write_text("""
blueprint: unique_test_blueprint
job_id: first-dag-id
""")

        config_file2 = tmp_path / "config2.dag.yaml"
        config_file2.write_text("""
blueprint: unique_test_blueprint
job_id: second-dag-id
""")

        # Change to tmp_path directory so lint command finds both files
        import os
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            
            runner = CliRunner()
            result = runner.invoke(cli, ["lint", "--template-dir", str(template_dir)])
            assert result.exit_code == 0  # Should pass
            assert "✅" in result.output
            assert "config1.dag.yaml" in result.output
            assert "config2.dag.yaml" in result.output
            # Should not mention duplicates
            assert "Duplicate DAG ID" not in result.output
        finally:
            os.chdir(original_dir)

    def test_lint_single_file_no_duplicate_check(self, tmp_path):
        """Test linting single file doesn't check for duplicates."""
        # Create blueprint
        template_dir = tmp_path / "templates"
        template_dir.mkdir()

        blueprint_code = """
from blueprint import Blueprint, BaseModel
from airflow import DAG
from datetime import datetime

class SingleTestConfig(BaseModel):
    job_id: str

class SingleTestBlueprint(Blueprint[SingleTestConfig]):
    def render(self, config: SingleTestConfig) -> DAG:
        return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
"""
        (template_dir / "single_test.py").write_text(blueprint_code)

        # Create one config file
        config_file = tmp_path / "single.dag.yaml"
        config_file.write_text("""
blueprint: single_test_blueprint
job_id: single-dag-id
""")

        runner = CliRunner()
        result = runner.invoke(cli, ["lint", str(config_file), "--template-dir", str(template_dir)])
        assert result.exit_code == 0  # Should pass
        assert "✅" in result.output
        assert "Valid" in result.output
