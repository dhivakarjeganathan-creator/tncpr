"""
Database configuration and connection management for Batch Analytics.
Compatible with Python 3.12+
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dataclasses import dataclass

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If python-dotenv is not installed, continue without it
    pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration class"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int
    max_connections: int

class DatabaseManager:
    """Database connection manager with connection pooling"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        if config is None:
            # Import here to avoid circular imports
            from env_config import env_config
            config = env_config.get_database_config()
        self.config = config
        self.connection_pool: Optional[SimpleConnectionPool] = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            self.connection_pool = SimpleConnectionPool(
                minconn=self.config.min_connections,
                maxconn=self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool"""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if connection:
                try:
                    self.connection_pool.putconn(connection)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute a SELECT query and return results"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
    
    def execute_batch(self, query: str, params_list: list) -> int:
        """Execute a batch of queries"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            logger.info(f"Testing connection to {self.config.database} on {self.config.host}:{self.config.port}")
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 as test")
                    result = cursor.fetchone()
                    logger.info("Connection test successful")
                    # Handle both dict (RealDictCursor) and tuple results
                    if isinstance(result, dict):
                        return result['test'] == 1
                    else:
                        return result[0] == 1
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            logger.error("Possible causes:")
            logger.error("1. Database does not exist")
            logger.error("2. User does not have permission")
            logger.error("3. Wrong password")
            logger.error("4. PostgreSQL server not running")
            logger.error("5. Wrong host/port")
            return False
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def close_pool(self):
        """Close the connection pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_pool()

# Global database manager instance
db_manager: Optional[DatabaseManager] = None

def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager()  # Will use env_config automatically
    return db_manager

def initialize_database(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """Initialize the database manager with given configuration"""
    global db_manager
    db_manager = DatabaseManager(config)
    return db_manager
