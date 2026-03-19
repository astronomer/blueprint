"""Integration test fixtures for Astro CLI standalone mode.

Manages the Astro project lifecycle: writes requirements, starts/stops Airflow,
and provides an authenticated API client for test interactions.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

ASTRO_CLI = os.environ.get("ASTRO_CLI", "astro")
INTEGRATION_DIR = Path(__file__).parent
PROJECT_DIR = INTEGRATION_DIR / "project"
REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_PORT = "18080"

HEALTH_CHECK_TIMEOUT = 120
HEALTH_CHECK_INTERVAL = 2
DAG_PARSE_TIMEOUT = 60
ASTRO_START_TIMEOUT = 600

EXPECTED_DAG_IDS = {"simple_pipeline", "versioned_etl", "dag_args_test", "explicit_naming"}


# ---------------------------------------------------------------------------
# API wrapper
# ---------------------------------------------------------------------------


@dataclass
class AirflowAPI:
    """Wrapper over the Airflow REST API with auth handling."""

    base_url: str
    _client: httpx.Client | None = field(default=None, repr=False)

    def client(self) -> httpx.Client:
        if self._client is None:
            token = self._get_jwt_token()
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=30,
                headers={"Authorization": f"Bearer {token}"},
            )
        return self._client

    def _get_jwt_token(self) -> str:
        resp = httpx.post(
            f"{self.base_url}/auth/token",
            data={"username": "admin", "password": "admin"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def get(self, path: str) -> httpx.Response:
        return self.client().get(f"/api/v2{path}")

    def patch(self, path: str, **kwargs) -> httpx.Response:
        return self.client().patch(f"/api/v2{path}", **kwargs)

    def post(self, path: str, **kwargs) -> httpx.Response:
        return self.client().post(f"/api/v2{path}", **kwargs)

    def get_dag_ids(self) -> set[str]:
        resp = self.get("/dags")
        assert resp.status_code == 200, resp.text
        return {d["dag_id"] for d in resp.json()["dags"]}

    def get_tags(self, dag: dict) -> set[str]:
        return {t["name"] for t in dag.get("tags", [])}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    return env


def _run_astro(
    *args: str,
    project_dir: Path = PROJECT_DIR,
    check: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [ASTRO_CLI, *args],
        cwd=str(project_dir),
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
        env=_clean_env(),
    )


def _wait_for_health(base_url: str) -> None:
    health_url = f"{base_url}/api/v2/monitor/health"
    deadline = time.monotonic() + HEALTH_CHECK_TIMEOUT

    while time.monotonic() < deadline:
        try:
            resp = httpx.get(health_url, timeout=5)
            if resp.status_code == 200:
                return
        except (httpx.ConnectError, httpx.RemoteProtocolError):
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)

    msg = f"Airflow did not become healthy within {HEALTH_CHECK_TIMEOUT}s at {health_url}"
    raise TimeoutError(msg)


def _wait_for_clean_dags(api: AirflowAPI) -> None:
    """Wait for the scheduler to clear all import errors and reload all expected DAGs."""
    deadline = time.monotonic() + DAG_PARSE_TIMEOUT
    while time.monotonic() < deadline:
        try:
            resp = api.get("/importErrors")
            has_errors = resp.status_code != 200 or resp.json().get("import_errors", [])
            if not has_errors and api.get_dag_ids() >= EXPECTED_DAG_IDS:
                return
        except Exception:
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)

    msg = f"DAGs did not recover within {DAG_PARSE_TIMEOUT}s"
    raise TimeoutError(msg)


def _wait_for_dags(api: AirflowAPI) -> None:
    deadline = time.monotonic() + DAG_PARSE_TIMEOUT
    while time.monotonic() < deadline:
        try:
            dag_ids = api.get_dag_ids()
            if dag_ids >= EXPECTED_DAG_IDS:
                return
        except Exception:
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)

    msg = f"Expected DAGs {EXPECTED_DAG_IDS} not found within {DAG_PARSE_TIMEOUT}s"
    raise TimeoutError(msg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def airflow_env():
    """Start a fresh Airflow standalone instance, yield an AirflowAPI, kill on teardown."""
    req_file = PROJECT_DIR / "requirements.txt"
    req_file.write_text(f"-e {REPO_ROOT}\n")

    _run_astro("dev", "kill", "--standalone", check=False)
    result = _run_astro(
        "dev",
        "start",
        "--standalone",
        "--no-proxy",
        "--no-browser",
        "-p",
        AIRFLOW_PORT,
        check=False,
        timeout=ASTRO_START_TIMEOUT,
    )
    if result.returncode != 0:
        msg = (
            f"astro dev start failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(msg)

    base_url = f"http://localhost:{AIRFLOW_PORT}"
    _wait_for_health(base_url)

    api = AirflowAPI(base_url=base_url)
    try:
        _wait_for_dags(api)
        yield api
    finally:
        api.close()
        _run_astro("dev", "kill", "--standalone", check=False)


@pytest.fixture(scope="session")
def api_client(airflow_env: AirflowAPI) -> AirflowAPI:
    """Provide the API wrapper to tests."""
    return airflow_env
