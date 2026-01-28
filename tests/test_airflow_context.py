"""Tests for the Airflow context provider."""

import os
from unittest.mock import MagicMock, patch

import pytest

from blueprint.airflow_context import (
    ConnectionProxy,
    ConnectionWrapper,
    EnvironmentProxy,
    VariableProxy,
    get_airflow_template_context,
)


class TestEnvironmentProxy:
    """Tests for EnvironmentProxy."""

    def test_getattr_existing_var(self, monkeypatch):
        """Test accessing an existing environment variable."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        proxy = EnvironmentProxy()
        assert proxy.TEST_VAR == "test_value"

    def test_getattr_missing_var(self):
        """Test accessing a missing environment variable returns empty string."""
        proxy = EnvironmentProxy()
        # Use a var name that definitely doesn't exist
        assert proxy.DEFINITELY_NOT_A_REAL_VAR_12345 == ""

    def test_get_with_default(self):
        """Test get method with default value."""
        proxy = EnvironmentProxy()
        assert proxy.get("MISSING_VAR", "default") == "default"

    def test_get_existing(self, monkeypatch):
        """Test get method with existing variable."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        proxy = EnvironmentProxy()
        assert proxy.get("TEST_VAR", "default") == "test_value"

    def test_getitem(self, monkeypatch):
        """Test dictionary-style access."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        proxy = EnvironmentProxy()
        assert proxy["TEST_VAR"] == "test_value"


class TestVariableProxy:
    """Tests for VariableProxy."""

    def test_value_accessor_without_airflow(self):
        """Test value accessor when Airflow is not available."""
        proxy = VariableProxy()
        # Force reload of variable class
        proxy._variable_class = None

        with patch.dict("sys.modules", {"airflow.models": None}):
            proxy._variable_class = False
            assert proxy.value.some_key == ""

    def test_value_accessor_with_airflow(self):
        """Test value accessor with mocked Airflow Variable."""
        mock_variable = MagicMock()
        mock_variable.get.return_value = "test_value"

        proxy = VariableProxy()
        proxy._variable_class = mock_variable

        assert proxy.value.my_key == "test_value"
        mock_variable.get.assert_called_with("my_key", default_var="")

    def test_json_accessor_with_airflow(self):
        """Test JSON accessor with mocked Airflow Variable."""
        mock_variable = MagicMock()
        mock_variable.get.return_value = {"key": "value"}

        proxy = VariableProxy()
        proxy._variable_class = mock_variable

        result = proxy.json.my_json_var
        assert result == {"key": "value"}
        mock_variable.get.assert_called_with("my_json_var", deserialize_json=True, default_var={})

    def test_get_method(self):
        """Test get method."""
        mock_variable = MagicMock()
        mock_variable.get.return_value = "test_value"

        proxy = VariableProxy()
        proxy._variable_class = mock_variable

        assert proxy.get("my_key", "default") == "test_value"


class TestConnectionProxy:
    """Tests for ConnectionProxy."""

    def test_getattr_returns_wrapper(self):
        """Test that attribute access returns a ConnectionWrapper."""
        proxy = ConnectionProxy()
        wrapper = proxy.my_conn
        assert isinstance(wrapper, ConnectionWrapper)
        assert wrapper._conn_id == "my_conn"


class TestConnectionWrapper:
    """Tests for ConnectionWrapper."""

    def test_properties_without_airflow(self):
        """Test properties when Airflow is not available."""
        wrapper = ConnectionWrapper("test_conn", None)
        assert wrapper.host == ""
        assert wrapper.login == ""
        assert wrapper.password == ""
        assert wrapper.schema == ""
        assert wrapper.port is None
        assert wrapper.conn_type == ""
        assert wrapper.extra == ""
        assert wrapper.extra_dejson == {}

    def test_properties_with_airflow(self):
        """Test properties with mocked connection."""
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_conn.host = "localhost"
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.schema = "mydb"
        mock_conn.port = 5432
        mock_conn.conn_type = "postgres"
        mock_conn.extra = '{"key": "value"}'
        mock_conn.extra_dejson = {"key": "value"}
        mock_conn.get_uri.return_value = "postgres://user:pass@localhost:5432/mydb"
        mock_hook.get_connection.return_value = mock_conn

        wrapper = ConnectionWrapper("test_conn", mock_hook)

        assert wrapper.host == "localhost"
        assert wrapper.login == "user"
        assert wrapper.password == "pass"
        assert wrapper.schema == "mydb"
        assert wrapper.port == 5432
        assert wrapper.conn_type == "postgres"
        assert wrapper.extra == '{"key": "value"}'
        assert wrapper.extra_dejson == {"key": "value"}
        assert str(wrapper) == "postgres://user:pass@localhost:5432/mydb"

    def test_str_without_connection(self):
        """Test string representation without connection."""
        wrapper = ConnectionWrapper("test_conn", None)
        assert str(wrapper) == ""


class TestGetAirflowTemplateContext:
    """Tests for get_airflow_template_context."""

    def test_returns_required_keys(self):
        """Test that context contains required keys."""
        context = get_airflow_template_context()

        assert "env" in context
        assert "var" in context
        assert "conn" in context
        assert isinstance(context["env"], EnvironmentProxy)
        assert isinstance(context["var"], VariableProxy)
        assert isinstance(context["conn"], ConnectionProxy)

    def test_extra_context(self):
        """Test that extra context is merged."""
        extra = {"custom_key": "custom_value", "another": 123}
        context = get_airflow_template_context(extra_context=extra)

        assert context["custom_key"] == "custom_value"
        assert context["another"] == 123
        assert "env" in context  # Original keys still present

    def test_extra_context_overrides(self):
        """Test that extra context can override defaults."""
        extra = {"env": "overridden"}
        context = get_airflow_template_context(extra_context=extra)

        assert context["env"] == "overridden"
