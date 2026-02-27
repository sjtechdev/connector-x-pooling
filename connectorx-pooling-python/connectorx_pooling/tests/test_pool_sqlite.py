"""SQLite connection pool integration tests."""
import os

import pytest
from pandas.testing import assert_frame_equal

from .. import ConnectionPool, read_sql


@pytest.fixture(scope="module")  # type: ignore
def sqlite_url() -> str:
    return os.environ["SQLITE_URL"]


@pytest.fixture  # type: ignore
def pool(sqlite_url: str):
    p = ConnectionPool(sqlite_url, max_size=5)
    yield p
    if not p.is_closed:
        p.close()


# ---------------------------------------------------------------------------
# Pool object properties
# ---------------------------------------------------------------------------


def test_pool_properties(pool: ConnectionPool):
    assert pool.max_size == 5
    assert pool.is_closed is False
    assert pool.default_protocol == "binary"
    assert repr(pool) == "ConnectionPool(max_size=5, status=open)"


# ---------------------------------------------------------------------------
# Basic query execution
# ---------------------------------------------------------------------------


def test_pool_single_query(pool: ConnectionPool):
    df = read_sql(pool, "SELECT test_int FROM test_table")
    assert len(df) == 6
    assert "test_int" in df.columns


def test_pool_reuse(pool: ConnectionPool):
    """The same pool instance works across multiple read_sql calls."""
    df1 = read_sql(pool, "SELECT test_int FROM test_table limit 3")
    df2 = read_sql(pool, "SELECT test_int FROM test_table limit 3")
    assert_frame_equal(df1, df2)
    assert not pool.is_closed


# ---------------------------------------------------------------------------
# Partitioned queries
# ---------------------------------------------------------------------------


def test_pool_partitioned_query(pool: ConnectionPool):
    df = read_sql(
        pool,
        "SELECT test_int FROM test_table",
        partition_on="test_int",
        partition_num=3,
    )
    assert len(df) == 6


def test_pool_partition_num_exceeds_max_size_raises(sqlite_url: str):
    p = ConnectionPool(sqlite_url, max_size=3)
    try:
        with pytest.raises(ValueError, match="partition_num"):
            read_sql(
                p,
                "SELECT test_int FROM test_table",
                partition_on="test_int",
                partition_num=5,
            )
    finally:
        p.close()


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


def test_pool_arrow_return_type(pool: ConnectionPool):
    import pyarrow as pa

    result = read_sql(pool, "SELECT test_int FROM test_table", return_type="arrow")
    assert isinstance(result, pa.Table)
    assert result.num_rows == 6


def test_pool_polars_return_type(pool: ConnectionPool):
    polars = pytest.importorskip("polars")
    result = read_sql(pool, "SELECT test_int FROM test_table", return_type="polars")
    assert isinstance(result, polars.DataFrame)
    assert len(result) == 6


# ---------------------------------------------------------------------------
# Lifecycle: context manager and explicit close
# ---------------------------------------------------------------------------


def test_pool_context_manager(sqlite_url: str):
    with ConnectionPool(sqlite_url, max_size=3) as p:
        df = read_sql(p, "SELECT test_int FROM test_table limit 1")
        assert len(df) == 1
        assert not p.is_closed
    assert p.is_closed


def test_pool_closed_raises_on_read_sql(sqlite_url: str):
    p = ConnectionPool(sqlite_url, max_size=3)
    p.close()
    assert p.is_closed
    with pytest.raises(ValueError, match="closed"):
        read_sql(p, "SELECT 1")


def test_pool_close_is_idempotent(sqlite_url: str):
    p = ConnectionPool(sqlite_url, max_size=3)
    p.close()
    p.close()
    assert p.is_closed
