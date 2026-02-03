"""Airflow context provider for Jinja2 template rendering.

This module provides access to Airflow Variables, Connections, and environment
variables for use in Blueprint YAML template rendering at DAG parse time.
"""

import os
from typing import Any, Dict, Optional


class EnvironmentProxy:
    """Proxy for accessing environment variables in templates.

    Usage in templates:
        {{ env.MY_VAR }}
        {{ env.get('MY_VAR', 'default') }}
    """

    def __getattr__(self, name: str) -> str:
        return os.environ.get(name, "")

    def get(self, name: str, default: str = "") -> str:
        return os.environ.get(name, default)

    def __getitem__(self, name: str) -> str:
        return os.environ.get(name, "")


class VariableProxy:
    """Proxy for accessing Airflow Variables in templates.

    Usage in templates:
        {{ var.value.my_key }}
        {{ var.get('my_key', 'default') }}
        {{ var.json.my_json_var }}
    """

    def __init__(self):
        self._variable_class = None

    def _get_variable_class(self):
        if self._variable_class is None:
            try:
                from airflow.models import Variable

                self._variable_class = Variable
            except ImportError:
                self._variable_class = False
        return self._variable_class

    @property
    def value(self) -> "VariableValueAccessor":
        return VariableValueAccessor(self._get_variable_class())

    @property
    def json(self) -> "VariableJsonAccessor":
        return VariableJsonAccessor(self._get_variable_class())

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        var_class = self._get_variable_class()
        if not var_class:
            return default
        try:
            return var_class.get(key, default_var=default)
        except Exception:
            return default


class VariableValueAccessor:
    """Accessor for Airflow Variable values via attribute access."""

    def __init__(self, variable_class):
        self._variable_class = variable_class

    def __getattr__(self, name: str) -> str:
        if not self._variable_class:
            return ""
        try:
            return self._variable_class.get(name, default_var="")
        except Exception:
            return ""


class VariableJsonAccessor:
    """Accessor for Airflow Variable JSON values via attribute access."""

    def __init__(self, variable_class):
        self._variable_class = variable_class

    def __getattr__(self, name: str) -> Any:
        if not self._variable_class:
            return {}
        try:
            return self._variable_class.get(name, deserialize_json=True, default_var={})
        except Exception:
            return {}


class ConnectionProxy:
    """Proxy for accessing Airflow Connections in templates.

    Usage in templates:
        {{ conn.my_conn.host }}
        {{ conn.my_conn.login }}
        {{ conn.my_conn.schema }}
        {{ conn.my_conn.port }}
        {{ conn.my_conn.extra_dejson }}

    Note: Password is intentionally not exposed in templates for security reasons.
    If you need connection credentials, access them directly in your DAG code.
    """

    def __init__(self):
        self._connection_class = None

    def _get_connection_class(self):
        if self._connection_class is None:
            try:
                from airflow.hooks.base import BaseHook

                self._connection_class = BaseHook
            except ImportError:
                self._connection_class = False
        return self._connection_class

    def __getattr__(self, conn_id: str) -> "ConnectionWrapper":
        return ConnectionWrapper(conn_id, self._get_connection_class())


class ConnectionWrapper:
    """Wrapper for an Airflow Connection that provides attribute access."""

    def __init__(self, conn_id: str, hook_class):
        self._conn_id = conn_id
        self._hook_class = hook_class
        self._connection = None

    def _get_connection(self):
        if self._connection is None and self._hook_class:
            try:
                self._connection = self._hook_class.get_connection(self._conn_id)
            except Exception:
                self._connection = False
        return self._connection

    @property
    def host(self) -> str:
        conn = self._get_connection()
        return conn.host if conn else ""

    @property
    def login(self) -> str:
        conn = self._get_connection()
        return conn.login if conn else ""

    # Note: password property intentionally omitted for security reasons.
    # Access connection credentials directly in DAG code if needed.

    @property
    def schema(self) -> str:
        conn = self._get_connection()
        return conn.schema if conn else ""

    @property
    def port(self) -> Optional[int]:
        conn = self._get_connection()
        return conn.port if conn else None

    @property
    def conn_type(self) -> str:
        conn = self._get_connection()
        return conn.conn_type if conn else ""

    @property
    def extra(self) -> str:
        conn = self._get_connection()
        return conn.extra if conn else ""

    @property
    def extra_dejson(self) -> Dict[str, Any]:
        conn = self._get_connection()
        return conn.extra_dejson if conn else {}

    def __str__(self) -> str:
        conn = self._get_connection()
        if conn:
            return conn.get_uri()
        return ""


def get_airflow_template_context(
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a context dict with Airflow Variables and Connections.

    This context is used for Jinja2 template rendering at DAG parse time.

    Args:
        extra_context: Additional context variables to include

    Returns:
        Dict with:
            - env: Environment variable accessor
            - var: Airflow Variable accessor
            - conn: Airflow Connection accessor
            - Plus any extra_context provided
    """
    context = {
        "env": EnvironmentProxy(),
        "var": VariableProxy(),
        "conn": ConnectionProxy(),
    }

    if extra_context:
        context.update(extra_context)

    return context
