"""Tests for the Blueprint configuration system."""

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

from blueprint.config import get_output_dir, get_template_path, load_config


class TestConfigLoading:
    """Test configuration file loading."""

    def test_load_config_file_not_found(self, tmp_path, chdir):
        """Test loading config when no config file exists."""
        with chdir(tmp_path):
            config = load_config()
            assert config == {}

    def test_load_config_blueprint_toml(self, tmp_path, chdir):
        """Test loading from blueprint.toml."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("""
[tool.blueprint]
template_path = "/custom/templates"
output_dir = "/custom/output"

[other]
value = "ignored"
""")

        with chdir(tmp_path):
            config = load_config()
            assert config["tool"]["blueprint"]["template_path"] == "/custom/templates"
            assert config["tool"]["blueprint"]["output_dir"] == "/custom/output"
            assert config["other"]["value"] == "ignored"

    def test_load_config_dot_blueprint_toml(self, tmp_path, chdir):
        """Test loading from .blueprint.toml (hidden file)."""
        config_file = tmp_path / ".blueprint.toml"
        config_file.write_text("""
template_path = "/hidden/templates"
output_dir = "/hidden/output"
""")

        with chdir(tmp_path):
            config = load_config()
            assert config["template_path"] == "/hidden/templates"
            assert config["output_dir"] == "/hidden/output"

    def test_load_config_precedence_blueprint_over_dot(self, tmp_path, chdir):
        """Test that blueprint.toml takes precedence over .blueprint.toml."""
        # Create both files
        (tmp_path / "blueprint.toml").write_text("""
template_path = "/primary/templates"
""")
        (tmp_path / ".blueprint.toml").write_text("""
template_path = "/secondary/templates"
""")

        with chdir(tmp_path):
            config = load_config()
            assert config["template_path"] == "/primary/templates"

    @pytest.mark.skipif(
        sys.version_info >= (3, 11), reason="Test for Python <3.11 tomli requirement"
    )
    def test_load_config_missing_tomli(self, tmp_path, chdir, monkeypatch):
        """Test error when tomli is not available on Python <3.11."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("template_path = '/test'")

        # Mock tomllib as None to simulate missing tomli
        monkeypatch.setattr("blueprint.config.tomllib", None)

        with chdir(tmp_path), pytest.raises(
            ImportError, match="tomli is required for Python <3.11"
        ):
            load_config()

    def test_load_config_invalid_toml(self, tmp_path, chdir):
        """Test error handling for invalid TOML syntax."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("""
template_path = "/test"
invalid_syntax =
""")

        with chdir(tmp_path), pytest.raises((ValueError, TypeError)):  # TOML parsing errors
            load_config()


class TestTemplatePath:
    """Test template path configuration with precedence."""

    def test_cli_value_precedence(self, tmp_path, chdir):
        """Test CLI value takes highest precedence."""
        # Create config file
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('template_path = "/config/templates"')

        with chdir(tmp_path), mock.patch.dict(
            os.environ, {"BLUEPRINT_TEMPLATE_PATH": "/env/templates"}
        ):
            path = get_template_path(cli_value="/cli/templates")
            assert path == "/cli/templates"

    def test_env_var_precedence(self, tmp_path, chdir):
        """Test environment variable takes precedence over config file."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('template_path = "/config/templates"')

        with chdir(tmp_path), mock.patch.dict(
            os.environ, {"BLUEPRINT_TEMPLATE_PATH": "/env/templates"}
        ):
            path = get_template_path()
            assert path == "/env/templates"

    def test_config_file_precedence(self, tmp_path, chdir):
        """Test config file takes precedence over default."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('template_path = "/config/templates"')

        with chdir(tmp_path), mock.patch.dict(os.environ, {}, clear=True):
            path = get_template_path()
            assert path == "/config/templates"

    def test_default_value(self, tmp_path, chdir):
        """Test default value when no config is provided."""
        with chdir(tmp_path), mock.patch.dict(os.environ, {}, clear=True):
            path = get_template_path()
            expected = str(Path("~/airflow").expanduser() / "dags")
            assert path == expected

    def test_custom_airflow_home(self, tmp_path, chdir):
        """Test default with custom AIRFLOW_HOME."""
        # Mock get_airflow_dags_folder to return custom path
        with chdir(tmp_path), mock.patch.dict(
            os.environ, {}, clear=True
        ), mock.patch(
            "blueprint.utils.get_airflow_dags_folder",
            return_value=Path("/custom/airflow/dags"),
        ):
            path = get_template_path()
            assert path == "/custom/airflow/dags"


class TestOutputDir:
    """Test output directory configuration."""

    def test_cli_value_precedence(self, tmp_path, chdir):
        """Test CLI value takes highest precedence."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('output_dir = "/config/output"')

        with chdir(tmp_path):
            path = get_output_dir(cli_value="/cli/output")
            assert path == "/cli/output"

    def test_config_file_precedence(self, tmp_path, chdir):
        """Test config file takes precedence over default."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('output_dir = "/config/output"')

        with chdir(tmp_path):
            path = get_output_dir()
            assert path == "/config/output"

    @mock.patch("blueprint.utils.get_airflow_dags_folder")
    def test_default_value(self, mock_get_dags_folder, tmp_path, chdir):
        """Test default value uses Airflow dags folder."""
        mock_dags_folder = Path("/airflow/dags")
        mock_get_dags_folder.return_value = mock_dags_folder

        with chdir(tmp_path):
            path = get_output_dir()
            assert path == "/airflow/dags/configs"
            mock_get_dags_folder.assert_called_once()


class TestConfigIntegration:
    """Test configuration integration with real scenarios."""

    def test_nested_config_structure(self, tmp_path, chdir):
        """Test TOML structure with nested sections."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("""
# Direct top-level config
template_path = "/direct/templates"
output_dir = "/direct/output"

[tool.blueprint]
template_path = "/project/templates"
output_dir = "/project/dags"

[tool.other]
value = "ignored"
""")

        with chdir(tmp_path), mock.patch.dict(os.environ, {}, clear=True):
            # The get_template_path should look for top-level template_path
            path = get_template_path()
            assert path == "/direct/templates"

            # Test that it loads the full config structure
            config = load_config()
            assert config["template_path"] == "/direct/templates"
            assert config["tool"]["blueprint"]["template_path"] == "/project/templates"

    def test_mixed_precedence_scenario(self, tmp_path, chdir):
        """Test realistic mixed precedence scenario."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("""
template_path = "/config/templates"
output_dir = "/config/output"
""")

        with chdir(tmp_path), mock.patch.dict(
            os.environ, {"BLUEPRINT_TEMPLATE_PATH": "/env/templates"}
        ):
            template_path = get_template_path()
            output_dir = get_output_dir()

            assert template_path == "/env/templates"
            assert output_dir == "/config/output"

    def test_empty_config_file(self, tmp_path, chdir):
        """Test handling of empty config file."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text("")

        with chdir(tmp_path):
            config = load_config()
            assert config == {}

            # Should fall back to defaults
            with mock.patch.dict(os.environ, {}, clear=True):
                template_path = get_template_path()
                expected = str(Path("~/airflow").expanduser() / "dags")
                assert template_path == expected

    def test_partial_config(self, tmp_path, chdir):
        """Test config with only some values set."""
        config_file = tmp_path / "blueprint.toml"
        config_file.write_text('template_path = "/only/templates"')

        with chdir(tmp_path), mock.patch(
            "blueprint.utils.get_airflow_dags_folder"
        ) as mock_get_dags_folder, mock.patch.dict(os.environ, {}, clear=True):
            mock_get_dags_folder.return_value = Path("/airflow/dags")

            template_path = get_template_path()
            output_dir = get_output_dir()

            assert template_path == "/only/templates"
            assert output_dir == "/airflow/dags/configs"
