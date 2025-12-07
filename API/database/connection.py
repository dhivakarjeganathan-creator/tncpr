"""
Database connection module supporting both PostgreSQL and Presto (Watsonx)
"""
import logging
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from config import Config, DatabaseType

logger = logging.getLogger(__name__)

# Conditionally import pyhive only if Presto is being used
_presto_available = False
try:
    from pyhive import presto
    _presto_available = True
except ImportError:
    logger.warning("pyhive not available. Presto connections will not work. Install with: pip install pyhive")


class DatabaseConnection:
    """Database connection manager supporting PostgreSQL and Presto"""
    
    def __init__(self):
        self.db_type = Config.DB_TYPE
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection based on type"""
        if self.db_type == DatabaseType.POSTGRESQL.value:
            try:
                self.connection_pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=Config.DB_HOST,
                    port=Config.DB_PORT,
                    database=Config.DB_NAME,
                    user=Config.DB_USER,
                    password=Config.DB_PASSWORD
                )
                logger.info("PostgreSQL connection pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
                raise
        elif self.db_type == DatabaseType.PRESTO.value:
            if not _presto_available:
                raise ImportError(
                    "Presto support requires pyhive. Install with: pip install pyhive"
                )
            logger.info("Presto connection will be created on-demand")
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        if self.db_type == DatabaseType.POSTGRESQL.value:
            conn = self.connection_pool.getconn()
            try:
                yield conn
            finally:
                self.connection_pool.putconn(conn)
        elif self.db_type == DatabaseType.PRESTO.value:
            if not _presto_available:
                raise ImportError(
                    "Presto support requires pyhive. Install with: pip install pyhive"
                )
            conn = presto.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                username=Config.DB_USER,
                catalog=Config.PRESTO_CATALOG or "hive",
                schema=Config.PRESTO_SCHEMA or Config.DB_NAME
            )
            try:
                yield conn
            finally:
                conn.close()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def execute_query(self, query: str, params: Optional[list] = None) -> list:
        """
        Execute a query and return results as list of dictionaries
        
        Args:
            query: SQL query string
            params: Optional query parameters list for parameterized queries (PostgreSQL)
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor if self.db_type == DatabaseType.POSTGRESQL.value else None)
                
                if self.db_type == DatabaseType.POSTGRESQL.value:
                    if params:
                        cursor.execute(query, tuple(params))
                    else:
                        cursor.execute(query)
                    results = cursor.fetchall()
                    # Convert to list of dicts
                    return [dict(row) for row in results]
                else:  # Presto
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in results]
                    
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def close(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")


# Global database connection instance
db_connection = DatabaseConnection()

