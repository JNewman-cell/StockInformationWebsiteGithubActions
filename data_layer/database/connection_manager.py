"""
Database connection manager for PostgreSQL.
"""

import os
import psycopg2
from psycopg2 import pool
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

from ..exceptions import DatabaseConnectionError


class DatabaseConnectionManager:
    """
    Manages PostgreSQL database connections with connection pooling.
    """
    
    def __init__(self, 
                 connection_string: Optional[str] = None,
                 min_connections: int = 1,
                 max_connections: int = 10):
        """
        Initialize the database connection manager.
        
        Args:
            connection_string: PostgreSQL connection string. If None, reads from DATABASE_URL env var.
            min_connections: Minimum number of connections in the pool.
            max_connections: Maximum number of connections in the pool.
        """
        self.logger = logging.getLogger(__name__)
        
        # Get connection string from parameter or environment
        self.connection_string = connection_string or os.getenv('DATABASE_URL')
        if not self.connection_string:
            raise DatabaseConnectionError(
                "No database connection string provided. Set DATABASE_URL environment variable "
                "or pass connection_string parameter."
            )
        
        self.min_connections = min_connections
        self.max_connections = max_connections
        self._connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        
    def _create_pool(self):
        """Create the connection pool."""
        try:
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                self.connection_string
            )
            self.logger.info(f"Created connection pool with {self.min_connections}-{self.max_connections} connections")
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create connection pool: {e}")
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            psycopg2.extensions.connection: Database connection
        """
        if self._connection_pool is None:
            self._create_pool()
        
        try:
            conn = self._connection_pool.getconn()
            if conn:
                return conn
            else:
                raise DatabaseConnectionError("No connection available in pool")
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to get connection from pool: {e}")
    
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: Database connection to return
        """
        if self._connection_pool and conn:
            try:
                self._connection_pool.putconn(conn)
            except Exception as e:
                self.logger.error(f"Failed to return connection to pool: {e}")
    
    @contextmanager
    def get_connection_context(self):
        """
        Context manager for database connections.
        Automatically returns the connection to the pool when done.
        
        Yields:
            psycopg2.extensions.connection: Database connection
        """
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    @contextmanager
    def get_cursor_context(self, commit: bool = True):
        """
        Context manager for database cursor with automatic connection management.
        
        Args:
            commit: Whether to commit the transaction automatically
        
        Yields:
            psycopg2.extensions.cursor: Database cursor
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.get_cursor_context() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None and result[0] == 1
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get database information.
        
        Returns:
            Dict containing database version and connection info
        """
        try:
            with self.get_cursor_context() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                
                cursor.execute("SELECT current_database()")
                database = cursor.fetchone()[0]
                
                cursor.execute("SELECT current_user")
                user = cursor.fetchone()[0]
                
                return {
                    'version': version,
                    'database': database,
                    'user': user,
                    'pool_size': f"{self.min_connections}-{self.max_connections}"
                }
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {}
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        if self._connection_pool:
            try:
                self._connection_pool.closeall()
                self.logger.info("Closed all database connections")
            except Exception as e:
                self.logger.error(f"Error closing connections: {e}")
    
    def __del__(self):
        """Cleanup when the object is destroyed."""
        self.close_all_connections()