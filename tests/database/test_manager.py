"""
Tests for the DatabaseManager class.

These tests focus on core database management functionality:
1. Singleton pattern
2. Database initialization
3. Session management
4. Connection handling
"""

import os
import tempfile
import pytest
import asyncio
from pathlib import Path

from alphatrade.database.manager import DatabaseManager
from alphatrade.database.models import PerformanceConfig
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

@pytest.mark.asyncio
async def test_singleton_pattern(db_path):
    """Test that DatabaseManager maintains singleton pattern."""
    # Create first instance
    db1 = DatabaseManager(db_path)
    
    # Create second instance with different parameters
    db2 = DatabaseManager("different.db", max_connections=10)
    
    # Should be the same instance
    assert db1 is db2
    assert db1.db_path == db2.db_path == db_path
    
    # Clean up
    await db1.close()

@pytest.mark.asyncio
async def test_database_initialization(db_path):
    """Test database initialization and setup."""
    db = DatabaseManager(db_path)
    
    # Setup should create tables
    await db.setup_database()
    
    async with db.connection.get_connection() as conn:
        # Check if core tables exist
        cursor = await conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table'
            """
        )
        tables = {row['name'] async for row in cursor}
        
        # Core tables should exist
        assert 'miners' in tables
        assert 'performance_snapshots' in tables
        
        # Check if indexes exist
        cursor = await conn.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='index'
            """
        )
        indexes = {row['name'] async for row in cursor}
        assert len(indexes) > 0  # Should have some indexes
    
    await db.close()

@pytest.mark.asyncio
async def test_session_management(db_path):
    """Test session creation and management."""
    db = DatabaseManager(db_path)
    await db.setup_database()
    
    # Test session context manager
    async with db.session() as session:
        # Session should be active
        assert not session.is_active  # Not in transaction
        
        # Start transaction
        async with session.begin():
            assert session.is_active  # In transaction
            
            # Test rollback
            await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await session.rollback()
        
        # Table should not exist after rollback
        async with db.connection.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='test'
                """
            )
            assert await cursor.fetchone() is None
    
    await db.close()

@pytest.mark.asyncio
async def test_connection_management(db_path):
    """Test connection handling and cleanup."""
    db = DatabaseManager(db_path)
    await db.setup_database()
    
    # Test multiple concurrent connections
    async def run_query(i: int):
        async with db.connection.get_connection() as conn:
            await conn.execute("SELECT 1")
            # Simulate some work
            await asyncio.sleep(0.1)
            return i
    
    # Run multiple queries concurrently
    tasks = [run_query(i) for i in range(5)]
    results = await asyncio.gather(*tasks)
    
    # All queries should complete
    assert len(results) == 5
    assert set(results) == set(range(5))
    
    await db.close()

@pytest.mark.asyncio
async def test_error_handling(db_path):
    """Test error handling in database operations."""
    db = DatabaseManager(db_path)
    await db.setup_database()
    
    # Test invalid SQL
    async with db.session() as session:
        with pytest.raises(DatabaseError):
            await session.execute("INVALID SQL")
    
    # Test constraint violation
    async with db.session() as session:
        with pytest.raises(DatabaseError):
            # Try to insert into non-existent table
            await session.execute("INSERT INTO nonexistent (id) VALUES (1)")
    
    await db.close()

@pytest.mark.asyncio
async def test_performance_config(db_path):
    """Test performance configuration handling."""
    config = PerformanceConfig(
        rolling_window_days=7,
        snapshot_interval_hours=6,
        retention_days=30
    )
    
    db = DatabaseManager(db_path, performance_config=config)
    await db.setup_database()
    
    # Config should be stored
    assert db.performance_config is config
    assert db.performance_config.rolling_window_days == 7
    assert db.performance_config.snapshot_interval_hours == 6
    assert db.performance_config.retention_days == 30
    
    await db.close()

@pytest.mark.asyncio
async def test_directory_creation(tmp_path):
    """Test database directory creation."""
    # Create deep path
    db_path = tmp_path / "deep" / "path" / "db.sqlite"
    
    # Directory should not exist
    assert not db_path.parent.exists()
    
    # Creating manager should create directory
    db = DatabaseManager(str(db_path))
    
    # Directory should exist
    assert db_path.parent.exists()
    
    await db.close() 