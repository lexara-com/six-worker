"""
Database Connection Pool with Retry Logic
Provides robust connection management for data loaders
"""
import psycopg2
from psycopg2 import pool, OperationalError
from psycopg2.extras import RealDictCursor
import logging
import time
from typing import Optional, Dict
from contextlib import contextmanager

class ConnectionPool:
    """Thread-safe connection pool with retry logic"""

    def __init__(self, connection_params: Dict[str, str],
                 min_connections: int = 1,
                 max_connections: int = 10,
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        """
        Initialize connection pool

        Args:
            connection_params: Database connection parameters
            min_connections: Minimum connections to maintain
            max_connections: Maximum connections to allow
            max_retries: Maximum retry attempts for failed operations
            retry_delay: Delay between retries in seconds
        """
        self.logger = logging.getLogger(__name__)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_params = connection_params

        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                cursor_factory=RealDictCursor,
                **connection_params
            )
            self.logger.info(f"✅ Connection pool initialized ({min_connections}-{max_connections} connections)")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self, auto_retry: bool = True):
        """
        Get a connection from the pool with automatic retry

        Args:
            auto_retry: If True, retry on connection failures

        Yields:
            Database connection
        """
        conn = None
        retries = 0
        last_error = None

        while retries <= (self.max_retries if auto_retry else 0):
            try:
                conn = self.pool.getconn()

                # Test connection
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")

                yield conn

                # Connection successful, return it to pool
                self.pool.putconn(conn)
                return

            except OperationalError as e:
                last_error = e
                self.logger.warning(f"Connection attempt {retries + 1} failed: {e}")

                # Close bad connection
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None

                retries += 1
                if retries <= self.max_retries:
                    time.sleep(self.retry_delay * retries)  # Exponential backoff

            except Exception as e:
                # Non-retryable error
                if conn:
                    self.pool.putconn(conn)
                raise

        # All retries exhausted
        raise OperationalError(f"Failed to get connection after {self.max_retries} retries: {last_error}")

    def close_all(self):
        """Close all connections in the pool"""
        try:
            self.pool.closeall()
            self.logger.info("Connection pool closed")
        except Exception as e:
            self.logger.error(f"Error closing connection pool: {e}")

    def __del__(self):
        """Cleanup on deletion"""
        self.close_all()
