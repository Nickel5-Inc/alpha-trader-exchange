"""
Tests for the DatabaseConnection class.

These tests focus on connection management functionality:
1. Connection creation and cleanup
2. Transaction management
3. Session handling
4. Concurrent access
"""

import os
import tempfile
import pytest
import asyncio
from pathlib import Path

from alphatrade.database.connection import DatabaseConnection
from alphatrade.database.exceptions import DatabaseError

@pytest.fixture
async def db_path():
    """Provide a temporary database path."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
        # Clean up WAL files if they exist
        for ext in ['-wal', '-shm']:
            wal_file = path + ext
            if os.path.exists(wal_file):
                os.unlink(wal_file)
    except OSError:
        pass

@pytest.fixture
async def connection(db_path):
    """Provide a database connection."""
    conn = DatabaseConnection(db_path)
    yield conn
    await conn.close()

@pytest.mark.asyncio
async def test_connection_creation(connection):
    """Test basic connection creation and settings."""
    async with connection.get_connection() as conn:
        # Test PRAGMA settings
        cursor = await conn.execute("PRAGMA journal_mode")
        result = await cursor.fetchone()
        assert result[0].upper() == "WAL"
        
        cursor = await conn.execute("PRAGMA synchronous")
        result = await cursor.fetchone()
        assert result[0] == 1  # NORMAL
        
        cursor = await conn.execute("PRAGMA foreign_keys")
        result = await cursor.fetchone()
        assert result[0] == 1  # ON

@pytest.mark.asyncio
async def test_connection_cleanup(connection):
    """Test connection cleanup on context exit."""
    async with connection.get_connection() as conn:
        # Connection should be open
        await conn.execute("SELECT 1")
    
    # Connection should be closed
    with pytest.raises(Exception):
        await conn.execute("SELECT 1")

@pytest.mark.asyncio
async def test_transaction_management(connection):
    """Test transaction management."""
    async with connection.get_connection() as conn:
        # Create test table
        await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Test successful transaction
        await conn.execute("BEGIN")
        await conn.execute("INSERT INTO test (id, value) VALUES (1, 'test')")
        await conn.commit()
        
        cursor = await conn.execute("SELECT value FROM test WHERE id = 1")
        result = await cursor.fetchone()
        assert result[0] == 'test'
        
        # Test rollback
        await conn.execute("BEGIN")
        await conn.execute("INSERT INTO test (id, value) VALUES (2, 'rollback')")
        await conn.rollback()
        
        cursor = await conn.execute("SELECT value FROM test WHERE id = 2")
        result = await cursor.fetchone()
        assert result is None

@pytest.mark.asyncio
async def test_session_management(connection):
    """Test session management."""
    async with connection.session() as session:
        # Create test table
        await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Test transaction
        async with session.begin():
            await session.execute("INSERT INTO test (id, value) VALUES (1, 'test')")
        
        # Verify data was committed
        result = await session.execute("SELECT value FROM test WHERE id = 1")
        row = (await result.fetchone())[0]
        assert row == 'test'

@pytest.mark.asyncio
async def test_concurrent_access(connection):
    """Test concurrent database access."""
    # Create test table
    async with connection.get_connection() as conn:
        await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    
    async def insert_value(i: int):
        async with connection.get_connection() as conn:
            await conn.execute("INSERT INTO test (id, value) VALUES (?, ?)", (i, f"value{i}"))
            await asyncio.sleep(0.1)  # Simulate work
    
    # Run concurrent inserts
    tasks = [insert_value(i) for i in range(5)]
    await asyncio.gather(*tasks)
    
    # Verify all values were inserted
    async with connection.get_connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM test")
        count = (await cursor.fetchone())[0]
        assert count == 5

@pytest.mark.asyncio
async def test_write_lock(connection):
    """Test write lock for serialization."""
    # Create test table
    async with connection.get_connection() as conn:
        await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, counter INTEGER)")
        await conn.execute("INSERT INTO test (id, counter) VALUES (1, 0)")
    
    async def increment_counter():
        async with connection._write_lock:
            async with connection.get_connection() as conn:
                # Read current value
                cursor = await conn.execute("SELECT counter FROM test WHERE id = 1")
                current = (await cursor.fetchone())[0]
                # Simulate some work
                await asyncio.sleep(0.1)
                # Increment
                await conn.execute(
                    "UPDATE test SET counter = ? WHERE id = 1",
                    (current + 1,)
                )
    
    # Run concurrent increments
    tasks = [increment_counter() for _ in range(5)]
    await asyncio.gather(*tasks)
    
    # Verify final value
    async with connection.get_connection() as conn:
        cursor = await conn.execute("SELECT counter FROM test WHERE id = 1")
        final = (await cursor.fetchone())[0]
        assert final == 5  # Each increment succeeded exactly once

@pytest.mark.asyncio
async def test_error_handling(connection):
    """Test error handling in transactions."""
    async def failing_transaction():
        async with connection.session() as session:
            async with session.begin():
                await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
                await session.execute("INSERT INTO test (id) VALUES (1)")
                # This should fail
                await session.execute("INSERT INTO nonexistent (id) VALUES (1)")
    
    # Transaction should roll back on error
    with pytest.raises(DatabaseError):
        await failing_transaction()
    
    # Table should not exist
    async with connection.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='test'
            """
        )
        assert await cursor.fetchone() is None

@pytest.mark.asyncio
async def test_max_connections(db_path):
    """Test maximum connection limit."""
    connection = DatabaseConnection(db_path, max_connections=2)
    
    async def hold_connection():
        async with connection.get_connection() as conn:
            await conn.execute("SELECT 1")
            await asyncio.sleep(0.5)  # Hold connection
    
    # First two connections should work
    task1 = asyncio.create_task(hold_connection())
    task2 = asyncio.create_task(hold_connection())
    
    # Third connection should wait
    start_time = asyncio.get_event_loop().time()
    task3 = asyncio.create_task(hold_connection())
    
    await asyncio.gather(task1, task2, task3)
    end_time = asyncio.get_event_loop().time()
    
    # Task3 should have waited for a connection
    assert end_time - start_time >= 0.5
    
    await connection.close() 