"""MySQL connection pool integration tests."""
import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from .. import ConnectionPool, read_sql


@pytest.fixture(scope="module")  # type: ignore
def mysql_url() -> str:
    return os.environ["MYSQL_URL"]


@pytest.fixture  # type: ignore
def pool(mysql_url: str):
    p = ConnectionPool(mysql_url, max_size=5)
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
    df = read_sql(pool, "SELECT * FROM test_table")
    assert len(df) == 6
    assert set(df.columns) == {"test_int", "test_float", "test_enum", "test_null"}


def test_pool_reuse(pool: ConnectionPool):
    """The same pool instance works across multiple read_sql calls."""
    df1 = read_sql(pool, "SELECT * FROM test_table limit 3")
    df2 = read_sql(pool, "SELECT * FROM test_table limit 3")
    assert_frame_equal(df1, df2)
    assert not pool.is_closed


# ---------------------------------------------------------------------------
# Partitioned queries
# ---------------------------------------------------------------------------


def test_pool_partitioned_query(pool: ConnectionPool):
    df = read_sql(
        pool,
        "SELECT * FROM test_table",
        partition_on="test_int",
        partition_range=(0, 2000),
        partition_num=3,
    )
    assert len(df) == 6
    df.sort_values("test_int", inplace=True, ignore_index=True)
    expected = pd.DataFrame(
        index=range(6),
        data={
            "test_int": pd.Series([1, 2, 3, 4, 5, 6], dtype="Int64"),
            "test_float": pd.Series([1.1, 2.2, 3.3, 4.4, 5.5, 6.6], dtype="float64"),
            "test_enum": pd.Series(
                ["odd", "even", "odd", "even", "odd", "even"], dtype="object"
            ),
            "test_null": pd.Series([None, None, None, None, None, None], dtype="Int64"),
        },
    )
    assert_frame_equal(df, expected, check_names=True)


def test_pool_partition_num_exceeds_max_size_raises(mysql_url: str):
    p = ConnectionPool(mysql_url, max_size=3)
    try:
        with pytest.raises(ValueError, match="partition_num"):
            read_sql(
                p,
                "SELECT * FROM test_table",
                partition_on="test_int",
                partition_range=(0, 10),
                partition_num=5,
            )
    finally:
        p.close()


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


def test_pool_arrow_return_type(pool: ConnectionPool):
    import pyarrow as pa

    result = read_sql(pool, "SELECT * FROM test_table", return_type="arrow")
    assert isinstance(result, pa.Table)
    assert result.num_rows == 6


def test_pool_polars_return_type(pool: ConnectionPool):
    polars = pytest.importorskip("polars")
    result = read_sql(pool, "SELECT * FROM test_table", return_type="polars")
    assert isinstance(result, polars.DataFrame)
    assert len(result) == 6


# ---------------------------------------------------------------------------
# Lifecycle: context manager and explicit close
# ---------------------------------------------------------------------------


def test_pool_context_manager(mysql_url: str):
    with ConnectionPool(mysql_url, max_size=3) as p:
        df = read_sql(p, "SELECT * FROM test_table limit 1")
        assert len(df) == 1
        assert not p.is_closed
    assert p.is_closed


def test_pool_closed_raises_on_read_sql(mysql_url: str):
    p = ConnectionPool(mysql_url, max_size=3)
    p.close()
    assert p.is_closed
    with pytest.raises(ValueError, match="closed"):
        read_sql(p, "SELECT 1")


def test_pool_close_is_idempotent(mysql_url: str):
    """Calling close() multiple times must not raise."""
    p = ConnectionPool(mysql_url, max_size=3)
    p.close()
    p.close()
    assert p.is_closed
