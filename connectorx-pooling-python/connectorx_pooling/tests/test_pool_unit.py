"""Unit tests for ConnectionPool that don't require a live database connection."""
import inspect

import pytest

from .. import ConnectionPool, read_sql


def test_connection_pool_importable():
    assert ConnectionPool is not None


def test_connection_pool_has_required_methods():
    for method in ("close", "__enter__", "__exit__"):
        assert callable(getattr(ConnectionPool, method, None)), f"Missing method: {method}"


def test_connection_pool_unsupported_backend_raises():
    """Backends without pool support (MSSQL, BigQuery, Trino) raise an error."""
    with pytest.raises(Exception):
        ConnectionPool("mssql://user:pass@server/db")


def test_read_sql_accepts_conn_param():
    sig = inspect.signature(read_sql)
    assert "conn" in sig.parameters


def test_connection_pool_min_idle_is_last_param():
    """min_idle is appended last so existing positional calls keep working."""
    sig = inspect.signature(ConnectionPool)
    params = list(sig.parameters)
    assert params[-1] == "min_idle"
    assert sig.parameters["min_idle"].default is None
