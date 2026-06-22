"""Tier 0: Airflow safe-mode DAG discovery.

Airflow's DAG file processor runs in *safe mode* by default: it only treats a
file as a potential DAG file if its raw contents contain both the ``airflow``
and ``dag`` substrings. A blueprint loader is otherwise a plain function call,
so the entry-point name has to carry both substrings on its own -- otherwise a
minimal one-line loader is silently skipped and no DAGs appear.

This exercises Airflow's *real* discovery heuristic
(``airflow.utils.file.might_contain_dag``), so it does not need a running
Airflow instance. It is the regression guard for the
``build_all_dags`` -> ``build_all_airflow_dags`` rename: the test writes the
bare-minimum loader (import + call, nothing else) and asserts Airflow would
pick it up.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.utils.file import might_contain_dag

pytestmark = pytest.mark.integration

# The bare-minimum loader: no `from airflow import DAG`, no docstring, nothing
# but the public entry point. This is exactly what `blueprint new` scaffolds
# and what the docs tell users to write.
BARE_MINIMUM_LOADER = "from blueprint import build_all_airflow_dags\nbuild_all_airflow_dags()\n"

# The pre-rename equivalent, kept to document *why* the rename was necessary.
LEGACY_LOADER = "from blueprint import build_all_dags\nbuild_all_dags()\n"


def _write(tmp_path: Path, content: str) -> str:
    loader = tmp_path / "loader.py"
    loader.write_text(content)
    return str(loader)


def test_bare_minimum_loader_is_discovered_by_safe_mode(tmp_path: Path):
    """The minimal `build_all_airflow_dags` loader satisfies Airflow's scanner."""
    loader = _write(tmp_path, BARE_MINIMUM_LOADER)
    assert might_contain_dag(loader, safe_mode=True), (
        "Airflow's safe-mode scanner skipped the bare-minimum loader; the entry "
        "point name must contain both 'airflow' and 'dag'."
    )


def test_legacy_loader_is_skipped_by_safe_mode(tmp_path: Path):
    """The old `build_all_dags` loader is skipped -- the regression we fixed."""
    loader = _write(tmp_path, LEGACY_LOADER)
    assert not might_contain_dag(loader, safe_mode=True), (
        "Expected the pre-rename loader to be skipped (it lacks the 'airflow' "
        "substring); if this now passes, safe-mode behaviour changed."
    )
