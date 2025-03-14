"""
Database connection management for the Alpha Trade Exchange subnet.

This module provides the core database connection functionality, including:
1. Connection pooling via SQLAlchemy
2. Transaction management
3. Session handling
4. Error handling
"""

import asyncio
import aiosqlite
from typing import Optional, Any, AsyncGenerator
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy.sql.expression import TextClause

import bittensor as bt
from .exceptions import DatabaseError, ConnectionError

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manager class for database connections.
    
    This class provides a high-level interface for database connections
    and transaction management using SQLAlchemy's connection pooling.
    """
    
    def __init__(
        self,
        db_path: str = "alpha_trade.db",
        max_connections: int = 5
    ):
        """Initialize database connection manager.
        
        Args:
            db_path: Path to SQLite database file
            max_connections: Maximum number of concurrent connections
        """
        self.db_path = db_path
        self._max_connections = max(1, min(max_connections, 10))  # Clamp between 1 and 10
        
        # Ensure database directory exists
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # Initialize write lock for serializing write operations
        self._write_lock = asyncio.Lock()

        # Create async engine with optimal settings
        self.engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            pool_size=max_connections,
            max_overflow=2,
            pool_timeout=60,
            pool_recycle=3600,
            echo=False
        )
        
        # Create session factory
        self._session_factory = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession
        )
    
    async def initialize(self):
        """Initialize the database connection.
        
        This method ensures the database file exists and is accessible,
        and sets up optimal SQLite pragmas for performance and concurrency.
        
        Returns:
            bool: True if initialization successful
            
        Raises:
            DatabaseError: If initialization fails
        """
        bt.logging.info(f"Initializing database connection to {self.db_path}")
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            
            # Test connection by creating, using, and closing it
            async with self.get_connection() as conn:
                # Set SQLite pragmas for optimal performance
                await conn.execute("PRAGMA busy_timeout = 60000")
                await conn.execute("PRAGMA journal_mode = WAL")
                await conn.execute("PRAGMA synchronous = NORMAL")
                await conn.execute("PRAGMA foreign_keys = ON")
                await conn.execute("PRAGMA cache_size = -20000")  # ~20MB cache
                
                # Verify connection works
                cursor = await conn.execute("SELECT sqlite_version()")
                version = await cursor.fetchone()
                bt.logging.info(f"Connected to SQLite version {version[0]}")
                
            bt.logging.info("Database connection initialized successfully")
            return True
        except Exception as e:
            bt.logging.error(f"Failed to initialize database connection: {str(e)}")
            raise DatabaseError(f"Failed to initialize database connection: {str(e)}")
    
    async def _create_connection(self) -> aiosqlite.Connection:
        """Create a new database connection with optimized settings."""
        try:
            conn = await aiosqlite.connect(self.db_path, isolation_level=None)
            await conn.execute("PRAGMA busy_timeout = 60000")
            await conn.execute("PRAGMA journal_mode = WAL")
            await conn.execute("PRAGMA synchronous = NORMAL")
            await conn.execute("PRAGMA foreign_keys = ON")
            conn.row_factory = aiosqlite.Row
            return conn
        except Exception as e:
            raise DatabaseError(f"Failed to create database connection: {str(e)}")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Get a database connection.
        
        This method provides a connection with automatic cleanup.
        The connection is returned to the pool when the context exits.
        """
        connection = await self._create_connection()
        try:
            yield connection
        finally:
            try:
                await connection.close()
            except Exception:
                pass

    async def close(self):
        """Close all database connections and cleanup.
        
        This method disposes of the SQLAlchemy engine and all its connections.
        """
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a database session.
        
        This context manager provides a session with automatic cleanup.
        """
        async with self._session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def _run_transaction(self, txn_func) -> Any:
        """Run a transaction with proper error handling.
        
        Args:
            txn_func: Async function that takes a session and returns a result
            
        Returns:
            Any: Result of the transaction function
            
        Raises:
            DatabaseError: If transaction fails
        """
        async with self._write_lock:
            async with self.session() as session:
                try:
                    async with session.begin():
                        result = await txn_func(session)
                        return result
                except Exception as e:
                    raise DatabaseError(f"Transaction failed: {str(e)}")

    async def _run_read_query(self, query_func) -> Any:
        """Execute a read-only query.
        
        Args:
            query_func: Async function that takes a session and returns a result
            
        Returns:
            Any: Result of the query function
            
        Raises:
            DatabaseError: If query fails
        """
        async with self.session() as session:
            try:
                result = await query_func(session)
                return result
            except Exception as e:
                raise DatabaseError(f"Read query failed: {str(e)}")

    async def execute(self, sql, parameters=None):
        """Execute a SQL query with proper handling of SQLAlchemy text objects.
        
        Args:
            sql: SQL query string or SQLAlchemy TextClause
            parameters: Optional query parameters
            
        Returns:
            Cursor: Query cursor
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            # Convert TextClause to string if needed
            if isinstance(sql, TextClause):
                # Extract the SQL text and parameters
                compiled = sql.compile(compile_kwargs={"literal_binds": False})
                sql = str(compiled)
                
                # If no parameters provided, use the ones from the TextClause
                if parameters is None:
                    parameters = {key: value for key, value in compiled.params.items()}
            
            # Ensure parameters is a dict or tuple
            if parameters is None:
                parameters = {}
            
            async with self.get_connection() as conn:
                return await conn.execute(sql, parameters)
                
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {str(e)}")