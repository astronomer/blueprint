"""Integration test fixtures for Astro CLI standalone mode.

Manages the Astro project lifecycle: writes requirements, starts/stops Airflow,
and provides an authenticated API client for test interactions.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import httpx
import pytest

ASTRO_CLI = "astro"
PROJECT_DIR = Path(__file__).parent / "project"
REPO_ROOT = Path(__file__).resolve().parents[2]
PROXY_DIR = Path.home() / ".astro" / "proxy"
PROXY_ROUTES_FILE = PROXY_DIR / "routes.json"
DEFAULT_PROXY_PORT = "6563"

HEALTH_CHECK_TIMEOUT = 120
HEALTH_CHECK_INTERVAL = 2


def _get_proxy_port() -> str:
    """Read the proxy daemon's listening port from its PID file."""
    pid_file = PROXY_DIR / "proxy.pid"
    if pid_file.exists():
        # Format: "<pid> <version>" — port is separate config, use default
        pass
    return DEFAULT_PROXY_PORT


def _discover_api_url() -> str:
    """Discover the API URL from the Astro proxy routes file."""
    if not PROXY_ROUTES_FILE.exists():
        return "http://localhost:8080"

    routes = json.loads(PROXY_ROUTES_FILE.read_text())
    project_dir_str = str(PROJECT_DIR.resolve())
    proxy_port = _get_proxy_port()
    for route in routes:
        if route.get("projectDir") == project_dir_str:
            hostname = route.get("hostname", "")
            if hostname:
                return f"http://{hostname}:{proxy_port}"
            port = route.get("port", "8080")
            return f"http://localhost:{port}"

    return "http://localhost:8080"


def _get_auth_token(base_url: str) -> str:
    """Get a JWT token from Airflow's SimpleAuthManager."""
    resp = httpx.post(
        f"{base_url}/auth/token",
        data={"username": "admin", "password": "admin"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _wait_for_health(base_url: str) -> None:
    """Poll the health endpoint until Airflow is ready."""
    health_url = f"{base_url}/api/v2/monitor/health"
    deadline = time.monotonic() + HEALTH_CHECK_TIMEOUT

    while time.monotonic() < deadline:
        try:
            resp = httpx.get(health_url, timeout=5)
            if resp.status_code == 200:
                return
        except httpx.ConnectError:
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)

    msg = f"Airflow did not become healthy within {HEALTH_CHECK_TIMEOUT}s at {health_url}"
    raise TimeoutError(msg)


ASTRO_START_TIMEOUT = 600


def _run_astro(
    *args: str,
    check: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess:
    """Run an astro CLI command from the project directory."""
    return subprocess.run(
        [ASTRO_CLI, *args],
        cwd=str(PROJECT_DIR),
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )


@pytest.fixture(scope="session")
def astro_project() -> Path:
    """Prepare the Astro project with a requirements.txt pointing to the local package."""
    req_file = PROJECT_DIR / "requirements.txt"
    req_file.write_text(f"-e {REPO_ROOT}\n")
    return PROJECT_DIR


@pytest.fixture(scope="session")
def airflow_running(astro_project):
    """Start a fresh Airflow standalone instance, yield API base URL, kill on teardown.

    Always kills any existing instance first to ensure a clean state. Uses `kill`
    (not `stop`) for teardown so no venv, database, or logs are left behind.
    """
    _run_astro("dev", "kill", "--standalone", check=False)
    result = _run_astro(
        "dev",
        "start",
        "--standalone",
        check=False,
        timeout=ASTRO_START_TIMEOUT,
    )
    if result.returncode != 0:
        msg = (
            f"astro dev start failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(msg)
    base_url = _discover_api_url()
    _wait_for_health(base_url)
    yield base_url
    _run_astro("dev", "kill", "--standalone", check=False)


@pytest.fixture(scope="session")
def api_client(airflow_running):
    """Provide an authenticated httpx client for the running Airflow API."""
    token = _get_auth_token(airflow_running)
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(base_url=airflow_running, timeout=30, headers=headers) as client:
        yield client
