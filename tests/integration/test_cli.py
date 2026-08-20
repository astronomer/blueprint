"""Blueprint CLI integration tests.

These tests run blueprint CLI commands against the integration test project's
DAG files and blueprint definitions. No running Airflow instance needed.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from .conftest import INTEGRATION_DIR

pytestmark = pytest.mark.integration

DAGS_DIR = str(INTEGRATION_DIR / "project" / "dags")


def _run_blueprint(*args: str, columns: int | None = None) -> subprocess.CompletedProcess:
    """Run a blueprint CLI command against the test project's dags."""
    env = None
    if columns is not None:
        env = {
            **os.environ,
            "COLUMNS": str(columns),
        }  # Could avoid columns if we'd have JSON output
    return subprocess.run(
        ["uv", "run", "blueprint", *args],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


class TestList:
    def test_lists_all_blueprints(self):
        result = _run_blueprint("list", "--template-dir", DAGS_DIR)
        assert result.returncode == 0, f"blueprint list failed:\n{result.stderr}"
        for name in ["extract", "transform", "load"]:
            assert name in result.stdout.lower(), f"Expected '{name}' in list output"

    def test_shows_versions(self):
        result = _run_blueprint("list", "--template-dir", DAGS_DIR)
        assert result.returncode == 0
        assert "1" in result.stdout
        assert "2" in result.stdout

    def test_lists_entry_point_sourced_blueprint_with_dotted_location(self):
        """Verify if the entry-point blueprints are discovered."""
        result = _run_blueprint("list", "--template-dir", DAGS_DIR, columns=200)
        assert result.returncode == 0, f"blueprint list failed:\n{result.stderr}"
        assert "entry_point_bp_test" in result.stdout.lower()
        assert "entry_point_test_blueprints.entrypoint_bp_test" in result.stdout
        assert DAGS_DIR not in result.stdout

    def test_no_entry_points_flag_hides_installed_package_blueprint(self):
        result = _run_blueprint("list", "--template-dir", DAGS_DIR, "--no-entry-points")
        assert result.returncode == 0, f"blueprint list failed:\n{result.stderr}"
        assert "entry_point_test_blueprints.entrypoint_bp_test" not in result.stdout


class TestDescribe:
    def test_describe_extract(self):
        result = _run_blueprint("describe", "extract", "--template-dir", DAGS_DIR)
        assert result.returncode == 0, f"blueprint describe failed:\n{result.stderr}"
        assert "extract" in result.stdout.lower()

    def test_describe_extract_v1(self):
        result = _run_blueprint("describe", "extract", "-v", "1", "--template-dir", DAGS_DIR)
        assert result.returncode == 0
        assert "source_table" in result.stdout

    def test_describe_extract_v2(self):
        result = _run_blueprint("describe", "extract", "-v", "2", "--template-dir", DAGS_DIR)
        assert result.returncode == 0
        assert "sources" in result.stdout

    def test_describe_nonexistent(self):
        result = _run_blueprint("describe", "nonexistent", "--template-dir", DAGS_DIR)
        assert result.returncode != 0


class TestLint:
    def test_lint_valid_yaml(self):
        result = _run_blueprint(
            "lint",
            f"{DAGS_DIR}/simple.dag.yaml",
            "--template-dir",
            DAGS_DIR,
        )
        assert result.returncode == 0, f"blueprint lint failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_lint_all_yamls_in_dir(self):
        result = subprocess.run(
            ["uv", "run", "blueprint", "lint", "--template-dir", DAGS_DIR],
            capture_output=True,
            text=True,
            check=False,
            cwd=DAGS_DIR,
        )
        assert result.returncode == 0, f"blueprint lint failed:\n{result.stdout}"
        assert "PASS" in result.stdout

    def test_lint_versioned_yaml(self):
        result = _run_blueprint(
            "lint",
            f"{DAGS_DIR}/versioned.dag.yaml",
            "--template-dir",
            DAGS_DIR,
        )
        assert result.returncode == 0
        assert "PASS" in result.stdout

    def test_lint_skips_airflowignored_yaml(self):
        result = subprocess.run(
            ["uv", "run", "blueprint", "lint", "--template-dir", DAGS_DIR],
            capture_output=True,
            text=True,
            check=False,
            cwd=DAGS_DIR,
        )
        assert result.returncode == 0, f"blueprint lint failed:\n{result.stdout}"
        assert "airflowignore_excluded" not in result.stdout

    def test_lint_explicit_path_overrides_airflowignore(self):
        result = _run_blueprint(
            "lint",
            f"{DAGS_DIR}/ignored_dags/airflowignore_excluded.dag.yaml",
            "--template-dir",
            DAGS_DIR,
        )
        assert result.returncode == 0
        assert "PASS" in result.stdout


class TestSchema:
    def test_schema_extract(self):
        result = _run_blueprint("schema", "extract", "--template-dir", DAGS_DIR)
        assert result.returncode == 0, f"blueprint schema failed:\n{result.stderr}"
        assert "extract" in result.stdout.lower()

    def test_schema_transform(self):
        result = _run_blueprint("schema", "transform", "--template-dir", DAGS_DIR)
        assert result.returncode == 0
        assert "operations" in result.stdout

    def test_schema_nonexistent(self):
        result = _run_blueprint("schema", "nonexistent", "--template-dir", DAGS_DIR)
        assert result.returncode != 0

    def test_optional_field_is_plain_type_and_not_required(self):
        result = _run_blueprint("schema", "greet", "--template-dir", DAGS_DIR)
        assert result.returncode == 0, f"blueprint schema failed:\n{result.stderr}"

        schema = json.loads(result.stdout)
        suffix = schema["properties"]["suffix"]
        assert suffix["type"] == "string"
        assert "default" not in suffix
        assert "suffix" not in schema["required"]
        assert "anyOf" not in result.stdout

    def test_dag_args_optional_field_is_plain_type(self):
        result = _run_blueprint("schema", "--dag-args", "--template-dir", DAGS_DIR)
        assert result.returncode == 0, f"blueprint schema failed:\n{result.stderr}"

        schema = json.loads(result.stdout)
        assert schema["properties"]["schedule"]["type"] == "string"
        assert "schedule" not in schema["required"]
