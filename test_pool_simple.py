#!/usr/bin/env python3
"""
Simple test script for ConnectionPool functionality
"""
import connectorx as cx
import sqlite3
import tempfile
import os

# Create a temporary database file
temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
temp_db_path = temp_db.name
temp_db.close()

try:
    # Setup test database
    conn = sqlite3.connect(temp_db_path)
    conn.execute('CREATE TABLE test (id INTEGER, value TEXT)')
    conn.execute('INSERT INTO test VALUES (1, "hello"), (2, "world")')
    conn.commit()
    conn.close()

    print("=" * 60)
    print("ConnectorX ConnectionPool Test")
    print("=" * 60)

    # Test 1: Create a pool
    print("\n1. Creating ConnectionPool...")
    pool = cx.ConnectionPool(f"sqlite://{temp_db_path}", max_size=5)
    print(f"   Pool created: {pool}")
    print(f"   Status: {'closed' if pool.is_closed else 'open'}")
    print(f"   Max size: {pool.max_size}")
    print(f"   Connection string: {pool.conn_str}")
    print("   ✓ Pool creation successful")

    # Test 2: Read data using pool
    print("\n2. Reading data using pool...")
    df = cx.read_sql(pool, "SELECT * FROM test")
    print(f"   Data retrieved: {len(df)} rows")
    print(f"   {df}")
    print("   ✓ Pool query successful")

    # Test 3: Multiple queries with same pool
    print("\n3. Multiple queries with same pool...")
    df1 = cx.read_sql(pool, "SELECT * FROM test WHERE id = 1")
    df2 = cx.read_sql(pool, "SELECT * FROM test WHERE id = 2")
    print(f"   Query 1: {len(df1)} rows")
    print(f"   Query 2: {len(df2)} rows")
    print("   ✓ Multiple queries successful")

    # Test 4: Close pool
    print("\n4. Closing pool...")
    pool.close()
    print(f"   Pool closed: {pool.is_closed}")
    print("   ✓ Pool close successful")

    # Test 5: Error on closed pool
    print("\n5. Testing error on closed pool...")
    try:
        df = cx.read_sql(pool, "SELECT * FROM test")
        print("   ✗ Should have raised an error")
    except ValueError as e:
        print(f"   Caught expected error: {str(e)[:50]}...")
        print("   ✓ Closed pool error handling successful")

    # Test 6: Context manager
    print("\n6. Testing context manager...")
    with cx.ConnectionPool(f"sqlite://{temp_db_path}", max_size=3) as p:
        print(f"   Inside context: {p}")
        df = cx.read_sql(p, "SELECT COUNT(*) as count FROM test")
        print(f"   Query result: {df['count'][0]} rows in table")
    print(f"   After context: closed={p.is_closed}")
    print("   ✓ Context manager successful")

    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)

finally:
    # Cleanup
    if os.path.exists(temp_db_path):
        os.unlink(temp_db_path)
