"""
Environment configuration for Batch Analytics.
This file contains all configuration settings that can be loaded from environment variables.
"""

import os
from typing import Optional
from database_config import DatabaseConfig

class EnvConfig:
    """Environment configuration class"""
    
    def __init__(self):
        # Database Configuration
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = int(os.getenv('DB_PORT', '5432'))
        self.DB_NAME = os.getenv('DB_NAME', 'hierarchy_db')
        self.DB_USER = os.getenv('DB_USER', 'postgres')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', 'test@1234')
        
        # Connection Pool Settings
        self.DB_MIN_CONNECTIONS = int(os.getenv('DB_MIN_CONNECTIONS', '1'))
        self.DB_MAX_CONNECTIONS = int(os.getenv('DB_MAX_CONNECTIONS', '10'))
        
        # File Paths
        self.JSON_FILE_PATH = os.getenv('JSON_FILE_PATH', 'Batch_analytics.json')
        self.SCHEMA_FILE_PATH = os.getenv('SCHEMA_FILE_PATH', 'database_schema.sql')
        
        # Logging Configuration
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Application Settings
        self.DEFAULT_TIMEZONE = os.getenv('DEFAULT_TIMEZONE', 'UTC')
        self.ENABLE_DEBUG = os.getenv('ENABLE_DEBUG', 'False').lower() == 'true'
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration object"""
        return DatabaseConfig(
            host=self.DB_HOST,
            port=self.DB_PORT,
            database=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            min_connections=self.DB_MIN_CONNECTIONS,
            max_connections=self.DB_MAX_CONNECTIONS
        )
    
    def get_logging_config(self) -> dict:
        """Get logging configuration"""
        return {
            'level': self.LOG_LEVEL,
            'format': self.LOG_FORMAT
        }

# Global configuration instance
env_config = EnvConfig()

# Example .env file content
ENV_EXAMPLE_CONTENT = """# Batch Analytics Configuration
# Copy this file to .env and update with your actual values

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=test@1234

# Connection Pool Settings
DB_MIN_CONNECTIONS=1
DB_MAX_CONNECTIONS=10

# File Paths
JSON_FILE_PATH=Batch_analytics.json
SCHEMA_FILE_PATH=database_schema.sql

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s

# Application Settings
DEFAULT_TIMEZONE=UTC
ENABLE_DEBUG=False
"""

def create_env_file():
    """Create .env file from template"""
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(ENV_EXAMPLE_CONTENT)
        print("Created .env file from template")
    else:
        print(".env file already exists")

if __name__ == "__main__":
    create_env_file()
