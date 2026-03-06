"""Blueprint CLI integration tests.

These tests run blueprint CLI commands against the integration test project's
DAG files and blueprint definitions. No running Airflow instance needed.
"""

from __future__ import annotations

import subprocess

import pytest

from .conftest import PROJECT_DIR

pytestmark = pytest.mark.integration

DAGS_DIR = str(PROJECT_DIR / "dags")


def _run_blueprint(*args: str) -> subprocess.CompletedProcess:
    """Run a blueprint CLI command against the test project's dags."""
    return subprocess.run(
        ["uv", "run", "blueprint", *args],
        capture_output=True,
        text=True,
        check=False,
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
