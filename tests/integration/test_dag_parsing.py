"""Tier 1: DAG parsing tests.

These tests validate that DAGs parse without errors using `astro dev pytest --standalone`.
Requires the venv to be set up (astro dev start must have been run at least once).
"""

from __future__ import annotations

import pytest

from .conftest import PROJECT_DIR, _run_astro

pytestmark = pytest.mark.integration


class TestDagParsing:
    """Verify DAGs parse correctly via Astro CLI."""

    def test_astro_pytest_passes(self, astro_project):
        """All DAG integrity tests pass when run through astro dev pytest."""
        result = _run_astro("dev", "pytest", "--standalone", check=False)
        assert result.returncode == 0, (
            f"astro dev pytest failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_invalid_yaml_causes_failure(self, astro_project):
        """An invalid DAG YAML should cause a parse failure."""
        bad_yaml = PROJECT_DIR / "dags" / "_invalid_test.dag.yaml"
        bad_yaml.write_text(
            "dag_id: invalid_test\nsteps:\n  broken_step:\n    blueprint: nonexistent_blueprint\n"
        )
        try:
            result = _run_astro("dev", "pytest", "--standalone", check=False)
            assert result.returncode != 0, "Expected failure for invalid blueprint reference"
        finally:
            bad_yaml.unlink(missing_ok=True)
