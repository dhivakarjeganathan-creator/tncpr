"""
Database configuration and connection module for analytics data loading.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any
import logging

# Get logger (will use the logging configuration from the main script)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration and connection management."""
    
    def __init__(self, config_file: str = "db_config.env"):
        """Initialize database configuration."""
        self.config = self._load_config(config_file)
        self.connection = None
    
    def _load_config(self, config_file: str) -> Dict[str, str]:
        """Load database configuration from environment file."""
        config = {}
        
        # Default configuration
        default_config = {
            'DB_HOST': 'localhost',
            'DB_PORT': '5432',
            'DB_NAME': 'analytics_db',
            'DB_USER': 'postgres',
            'DB_PASSWORD': 'password'
        }
        
        # Load from environment file if exists
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        config[key] = value
        
        # Override with environment variables
        for key in default_config:
            config[key] = os.getenv(key, config.get(key, default_config[key]))
        
        return config
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(
                    host=self.config['DB_HOST'],
                    port=self.config['DB_PORT'],
                    database=self.config['DB_NAME'],
                    user=self.config['DB_USER'],
                    password=self.config['DB_PASSWORD']
                )
                logger.info("Database connection established successfully")
            except psycopg2.Error as e:
                logger.error(f"Error connecting to database: {e}")
                raise
        
        return self.connection
    
    def close_connection(self):
        """Close database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Database connection closed")
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return results."""
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if query.strip().upper().startswith(('SELECT', 'WITH')):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return cursor.rowcount
        except psycopg2.Error as e:
            logger.error(f"Error executing query: {e}")
            conn.rollback()
            raise
    
    def execute_batch(self, query: str, data: list) -> int:
        """Execute batch insert/update operations."""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.executemany(query, data)
                conn.commit()
                return cursor.rowcount
        except psycopg2.Error as e:
            logger.error(f"Error executing batch operation: {e}")
            conn.rollback()
            raise
