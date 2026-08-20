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


def _write_vars_project(
    root,
    dag_body,
    vars_body="profiles: [prod, dev]\nvars:\n  db:\n    prod: analytics\n    dev: sandbox\n",
):
    """Create a project with a vars file and one DAG that references it."""
    (root / "blueprint.vars.yaml").write_text(vars_body)
    (root / "provided.dag.yaml").write_text(dag_body)


DAG_USING_VARS = (
    "dag_id: vars_demo\nteam: platform\n\n"
    "vars:\n  suffix: _events\n\n"
    "steps:\n  load:\n    blueprint: load\n"
    "    target_table: ${db}.orders${suffix}\n"
)


class TestLintProfiles:
    """`blueprint lint` resolves declarative variables and profiles."""

    def test_lint_with_profile(self, tmp_path):
        _write_vars_project(tmp_path, DAG_USING_VARS)

        result = subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "lint",
                "provided.dag.yaml",
                "--profile",
                "prod",
                "--template-dir",
                DAGS_DIR,
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
        )

        assert result.returncode == 0, f"lint failed:\n{result.stdout}{result.stderr}"
        assert "PASS" in result.stdout

    def test_lint_every_declared_profile(self, tmp_path):
        _write_vars_project(tmp_path, DAG_USING_VARS)

        result = subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "lint",
                "provided.dag.yaml",
                "--template-dir",
                DAGS_DIR,
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
        )

        assert result.returncode == 0, f"lint failed:\n{result.stdout}{result.stderr}"
        assert result.stdout.count("PASS") == 2

    def test_lint_reports_missing_profile_value(self, tmp_path):
        _write_vars_project(
            tmp_path,
            DAG_USING_VARS,
            vars_body="profiles: [prod, dev]\nvars:\n  db:\n    prod: analytics\n",
        )

        result = subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "lint",
                "provided.dag.yaml",
                "--template-dir",
                DAGS_DIR,
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
        )

        assert result.returncode == 1
        assert "no value under profile 'dev'" in result.stdout

    def test_lint_reports_undefined_variable(self, tmp_path):
        _write_vars_project(
            tmp_path,
            "dag_id: vars_demo\nteam: platform\n\nvars:\n  a: 1\n\n"
            "steps:\n  load:\n    blueprint: load\n    target_table: ${nope}\n",
        )

        result = subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "lint",
                "provided.dag.yaml",
                "--profile",
                "prod",
                "--template-dir",
                DAGS_DIR,
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
        )

        assert result.returncode == 1
        assert "Undefined variable 'nope'" in result.stdout


class TestVarsCommand:
    def test_vars_shows_resolved_values_and_sources(self, tmp_path):
        _write_vars_project(tmp_path, DAG_USING_VARS)

        result = subprocess.run(
            ["uv", "run", "blueprint", "vars", "provided.dag.yaml", "--profile", "prod"],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
            env={**os.environ, "COLUMNS": "200"},
        )

        assert result.returncode == 0, f"vars failed:\n{result.stdout}{result.stderr}"
        assert "analytics" in result.stdout
        assert "blueprint.vars.yaml" in result.stdout
        assert "_events" in result.stdout

    def test_vars_reports_unused(self, tmp_path):
        _write_vars_project(
            tmp_path,
            "dag_id: vars_demo\nteam: platform\n\nvars:\n  never_used: x\n\n"
            "steps:\n  load:\n    blueprint: load\n    target_table: ${db}.t\n",
        )

        result = subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "vars",
                "provided.dag.yaml",
                "--profile",
                "prod",
                "--unused",
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
            env={**os.environ, "COLUMNS": "200"},
        )

        assert result.returncode == 0
        assert "never_used" in result.stdout
        assert "Not referenced by this DAG" in result.stdout


class TestLintDefaultProfile:
    """With no profile named, lint validates against every declared profile."""

    def _run(self, cwd, *args):
        return subprocess.run(
            [
                "uv",
                "run",
                "blueprint",
                "lint",
                "provided.dag.yaml",
                "--template-dir",
                DAGS_DIR,
                *args,
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(cwd),
        )

    def test_bare_lint_checks_every_profile(self, tmp_path):
        _write_vars_project(tmp_path, DAG_USING_VARS)

        result = self._run(tmp_path)

        assert result.returncode == 0, f"lint failed:\n{result.stdout}{result.stderr}"
        assert result.stdout.count("PASS") == 2

    def test_bare_lint_fails_if_any_profile_is_broken(self, tmp_path):
        _write_vars_project(
            tmp_path,
            DAG_USING_VARS,
            vars_body="profiles: [prod, dev]\nvars:\n  db:\n    prod: analytics\n",
        )

        result = self._run(tmp_path)

        assert result.returncode == 1
        assert "PASS" in result.stdout
        assert "no value under profile 'dev'" in result.stdout

    def test_unreferenced_varying_var_needs_no_profile(self, tmp_path):
        _write_vars_project(
            tmp_path,
            "dag_id: vars_demo\nteam: platform\n\n"
            "steps:\n  load:\n    blueprint: load\n    target_table: ${fixed}.t\n",
            vars_body="vars:\n  fixed: everywhere\n",
        )

        result = self._run(tmp_path)

        assert result.returncode == 0, f"lint failed:\n{result.stdout}{result.stderr}"
        assert "PASS" in result.stdout

    def test_vars_without_profile_shows_varying_markers(self, tmp_path):
        _write_vars_project(tmp_path, DAG_USING_VARS)

        result = subprocess.run(
            ["uv", "run", "blueprint", "vars", "provided.dag.yaml"],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
            env={**os.environ, "COLUMNS": "200"},
        )

        assert result.returncode == 0, f"vars failed:\n{result.stdout}{result.stderr}"
        assert "varies by profile" in result.stdout
        assert "no profile selected" in result.stdout
        assert "_events" in result.stdout
