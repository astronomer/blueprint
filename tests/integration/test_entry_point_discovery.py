"""
Test blueprint discovery from an installed package via entry points.

Validate if Blueprints from installed packages can be referenced by name with no corresponding
local .py file.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from .conftest import DAG_PARSE_TIMEOUT, HEALTH_CHECK_INTERVAL, PROJECT_DIR

if TYPE_CHECKING:
    from .conftest import AirflowAPI

pytestmark = pytest.mark.integration

ENTRY_POINT_DAG_ID = "entry_point_test"


class TestEntryPointDiscovery:
    """Verify a blueprint from an installed package (no local .py) is discovered."""

    def test_entry_point_sourced_dag_parses(self, api_client: AirflowAPI):
        deadline = time.monotonic() + DAG_PARSE_TIMEOUT
        dag_ids: set[str] = set()
        while time.monotonic() < deadline:
            dag_ids = api_client.get_dag_ids()
            if ENTRY_POINT_DAG_ID in dag_ids:
                break
            time.sleep(HEALTH_CHECK_INTERVAL)

        assert ENTRY_POINT_DAG_ID in dag_ids, (
            f"Airflow did not discover '{ENTRY_POINT_DAG_ID}'. Its blueprint "
            "(entry_point_bp_test) exists only in the installed test package "
            "-- entry-point discovery must be resolving it for this DAG to "
            "parse at all."
        )

    def test_no_import_errors_for_entry_point_dag(self, api_client: AirflowAPI):
        resp = api_client.get("/importErrors")
        assert resp.status_code == 200, resp.text
        offending = [
            e
            for e in resp.json().get("import_errors", [])
            if "entry_point_test" in (e.get("filename") or "")
        ]
        assert not offending, f"Import errors for the entry-point DAG: {offending}"

    def test_no_local_py_file_defines_the_test_blueprint(self):
        """Guard against a future 'fix' that quietly adds a local copy of the blueprint,
        which would defeat the entire point of this test suite without any single assertion
        above failing.
        """
        dags_dir = PROJECT_DIR / "dags"
        offenders = [
            py_file
            for py_file in dags_dir.rglob("*.py")
            if "class EntryPointBpTest(" in py_file.read_text()
        ]
        assert not offenders, (
            f"Found a local copy of EntryPointBpTest in {offenders}. The entry-point DAG must "
            "resolve its blueprint purely from the installed test package."
        )
