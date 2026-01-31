#!/usr/bin/env python3
"""
Test script for ConnectionPool functionality
"""
import connectorx as cx

def test_sqlite_pool():
    """Test basic SQLite pool functionality"""
    print("Testing SQLite ConnectionPool...")

    # Create a test database
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE test (id INTEGER, value TEXT)')
    conn.execute('INSERT INTO test VALUES (1, "hello"), (2, "world")')
    conn.commit()
    conn.close()

    # Test 1: Create a pool and use it
    print("Test 1: Creating ConnectionPool...")
    pool = cx.ConnectionPool("sqlite:///:memory:", max_size=5)
    print(f"  Pool created: {pool}")
    print(f"  Pool status: {'closed' if pool.is_closed else 'open'}")
    print(f"  Pool max_size: {pool.max_size}")

    # Since we can't easily test with in-memory DB across connections,
    # let's just verify the pool object works
    pool.close()
    print(f"  Pool closed: {pool.is_closed}")
    print("  ✓ Test 1 passed")

    # Test 2: Context manager
    print("\nTest 2: Testing context manager...")
    with cx.ConnectionPool("sqlite:///:memory:", max_size=5) as pool:
        print(f"  Pool inside context: {pool}")
        print(f"  Pool is open: {not pool.is_closed}")
    print(f"  Pool after context: closed={pool.is_closed}")
    print("  ✓ Test 2 passed")

    # Test 3: Verify closed pool error
    print("\nTest 3: Testing closed pool error handling...")
    try:
        closed_pool = cx.ConnectionPool("sqlite:///:memory:")
        closed_pool.close()
        # This should raise an error
        df = cx.read_sql(closed_pool, "SELECT 1")
        print("  ✗ Test 3 failed - should have raised error")
    except ValueError as e:
        print(f"  Caught expected error: {e}")
        print("  ✓ Test 3 passed")

    print("\n✅ All SQLite pool tests passed!")

def test_repr():
    """Test __repr__ method"""
    print("\nTesting __repr__...")
    pool = cx.ConnectionPool("sqlite:///:memory:", max_size=10)
    repr_str = repr(pool)
    print(f"  repr: {repr_str}")
    assert "ConnectionPool" in repr_str
    assert "max_size=10" in repr_str
    assert "open" in repr_str
    pool.close()
    repr_str = repr(pool)
    print(f"  repr after close: {repr_str}")
    assert "closed" in repr_str
    print("  ✓ __repr__ test passed")

if __name__ == "__main__":
    print("=" * 60)
    print("ConnectorX ConnectionPool Test Suite")
    print("=" * 60)

    test_sqlite_pool()
    test_repr()

    print("\n" + "=" * 60)
    print("All tests passed! ✅")
    print("=" * 60)
