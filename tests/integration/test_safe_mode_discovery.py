"""Tier 1: Airflow safe-mode DAG discovery.

Airflow's DAG file processor runs in *safe mode* by default: it only parses a
file as a potential DAG file when its contents contain both the ``airflow``
and ``dag`` substrings. A Blueprint loader is otherwise a plain function call,
so the entry-point name has to carry both substrings on its own -- otherwise a
minimal one-line loader is silently skipped and no DAGs appear.

This is the end-to-end regression guard for the
``build_all_dags`` -> ``build_all_airflow_dags`` rename. The project ships a
bare-minimum loader at ``dags/safe_mode_minimal/loader.py`` -- an import and a
call, with deliberately no ``from airflow import DAG`` -- and these tests run
against the live Airflow instance to assert that Airflow actually discovered
and parsed the DAG that loader builds.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from .conftest import DAG_PARSE_TIMEOUT, HEALTH_CHECK_INTERVAL

if TYPE_CHECKING:
    from .conftest import AirflowAPI

pytestmark = pytest.mark.integration

PROBE_DAG_ID = "safe_mode_minimal_probe"


class TestSafeModeDiscovery:
    """Verify the bare-minimum loader is discovered by the live Airflow scanner."""

    def test_bare_minimum_loader_dag_is_parsed(self, api_client: AirflowAPI):
        """Airflow parses the no-`import DAG` loader purely via the entry-point name."""
        deadline = time.monotonic() + DAG_PARSE_TIMEOUT
        dag_ids: set[str] = set()
        while time.monotonic() < deadline:
            dag_ids = api_client.get_dag_ids()
            if PROBE_DAG_ID in dag_ids:
                break
            time.sleep(HEALTH_CHECK_INTERVAL)

        assert PROBE_DAG_ID in dag_ids, (
            f"Airflow did not discover '{PROBE_DAG_ID}'. Its loader contains no "
            "'from airflow import DAG' — discovery relies solely on "
            "build_all_airflow_dags carrying the 'airflow' substring for safe mode."
        )

    def test_no_import_errors_for_minimal_loader(self, api_client: AirflowAPI):
        """The minimal loader parses cleanly, with no import error recorded."""
        resp = api_client.get("/importErrors")
        assert resp.status_code == 200, resp.text
        offending = [
            e
            for e in resp.json().get("import_errors", [])
            if "safe_mode_minimal" in (e.get("filename") or "")
        ]
        assert not offending, f"Import errors for the minimal loader: {offending}"
