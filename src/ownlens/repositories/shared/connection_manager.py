"""
Connection Manager for asyncpg
===============================

Simple connection manager wrapper for asyncpg PostgreSQL connections.
Provides execute_query and execute_update methods expected by repositories.
"""

import os
import asyncio
import logging
from typing import Dict, Any, List, Optional
import asyncpg

logger = logging.getLogger(__name__)


class AsyncPGConnectionManager:
    """
    Simple connection manager for asyncpg.
    
    Provides execute_query and execute_update methods expected by repositories.
    """
    
    def __init__(self, connection_string: str = None, **kwargs):
        """
        Initialize connection manager.
        
        Args:
            connection_string: PostgreSQL connection string (optional)
            **kwargs: Connection parameters (host, port, database, user, password)
        """
        self.connection_string = connection_string
        self.connection_params = kwargs
        self._pool: Optional[asyncpg.Pool] = None
        self._connection: Optional[asyncpg.Connection] = None
        self.logger = logging.getLogger(f"{__name__}.AsyncPGConnectionManager")
    
    async def connect(self):
        """Create a connection pool or single connection."""
        if self._pool is None and self._connection is None:
            if self.connection_string:
                self._pool = await asyncpg.create_pool(self.connection_string)
            else:
                # Get connection parameters from environment or kwargs
                host = self.connection_params.get('host') or os.getenv('POSTGRES_HOST', 'localhost')
                port = int(self.connection_params.get('port') or os.getenv('POSTGRES_PORT', '5432'))
                # If database is explicitly provided in kwargs, use it; otherwise use POSTGRES_DB
                # This allows using raw database for sample data generation
                database = self.connection_params.get('database') or os.getenv('POSTGRES_DB', 'ownlens')
                user = self.connection_params.get('user') or os.getenv('POSTGRES_USER', 'postgres')
                password = self.connection_params.get('password') or os.getenv('POSTGRES_PASSWORD', 'password')
                
                # Create a single connection for now (can be upgraded to pool later)
                self._connection = await asyncpg.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )
                self.logger.info(f"Connected to PostgreSQL at {host}:{port}/{database}")
    
    async def get_connection(self) -> asyncpg.Connection:
        """Get a connection from pool or return existing connection."""
        if not self._pool and not self._connection:
            await self.connect()
        
        if self._pool:
            return await self._pool.acquire()
        return self._connection
    
    async def release_connection(self, conn: asyncpg.Connection):
        """Release a connection back to pool."""
        if self._pool:
            await self._pool.release(conn)
        # For single connection, do nothing
    
    async def execute_query(self, query: str, params: List[Any] = None) -> Dict[str, Any]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Dictionary with 'rows' key containing list of result rows
        """
        if params is None:
            params = []
        
        conn = await self.get_connection()
        try:
            rows = await conn.fetch(query, *params)
            # Convert asyncpg.Record objects to dictionaries
            result_rows = [dict(row) for row in rows]
            return {"rows": result_rows}
        finally:
            await self.release_connection(conn)
    
    async def execute_update(self, query: str, params: List[Any] = None) -> Optional[Dict[str, Any]]:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Dictionary with result information or None
        """
        if params is None:
            params = []
        
        conn = await self.get_connection()
        try:
            # For INSERT with RETURNING, fetch the result
            if "RETURNING" in query.upper():
                try:
                    row = await conn.fetchrow(query, *params)
                    if row:
                        result_dict = dict(row)
                        return result_dict
                    else:
                        # fetchrow returned None - this could mean the INSERT failed or no row was returned
                        # Try to execute the query without RETURNING to see if it succeeds
                        logger.error(f"execute_update: RETURNING * returned None for query: {query[:200]}...")
                        # Check if the INSERT actually succeeded by trying to execute it without RETURNING
                        # This is a diagnostic step - in production, we'd want to handle this differently
                        return None
                except Exception as e:
                    # Check if this is a duplicate key error (expected when re-running script)
                    error_str = str(e)
                    is_duplicate = (
                        "duplicate key" in error_str.lower() or 
                        "unique constraint" in error_str.lower() or
                        "uniqueviolationerror" in error_str.lower()
                    )
                    if is_duplicate:
                        # Duplicate key errors are expected when re-running script - log at DEBUG level
                        logger.debug(f"execute_update: Duplicate key error (expected when re-running): {e}")
                    else:
                        # Other errors should be logged at ERROR level
                        logger.error(f"execute_update: Error executing query with RETURNING *: {e}")
                    raise
            else:
                # For UPDATE/DELETE, return affected row count
                result = await conn.execute(query, *params)
                # Parse result like "INSERT 0 1" or "UPDATE 1"
                parts = result.split()
                if len(parts) >= 2:
                    affected = int(parts[-1]) if parts[-1].isdigit() else 0
                    return {"affected": affected}
                return {"affected": 0}
        finally:
            await self.release_connection(conn)
    
    async def close(self):
        """Close all connections."""
        if self._connection:
            await self._connection.close()
            self._connection = None
        if self._pool:
            await self._pool.close()
            self._pool = None
        self.logger.info("Closed all connections")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


def create_connection_manager(**kwargs) -> AsyncPGConnectionManager:
    """
    Create a connection manager instance.
    
    Args:
        **kwargs: Connection parameters (host, port, database, user, password)
        
    Returns:
        AsyncPGConnectionManager instance
    """
    return AsyncPGConnectionManager(**kwargs)

