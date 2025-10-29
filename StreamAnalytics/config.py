"""
Configuration management for Streaming Analytics Loader
This module handles loading configuration from environment variables and .env files.
"""

import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file if it exists
load_dotenv()


class Config:
    """Configuration class for database and application settings."""
    
    # Database configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_NAME = os.getenv('DB_NAME', 'streaming_analytics')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    DB_PORT = os.getenv('DB_PORT', '5432')
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_db_config(cls) -> Dict[str, str]:
        """
        Get database configuration as a dictionary.
        
        Returns:
            Dictionary containing database connection parameters
        """
        return {
            'host': cls.DB_HOST,
            'database': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
            'port': cls.DB_PORT
        }
    
    @classmethod
    def validate_config(cls) -> bool:
        """
        Validate that all required configuration is present.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_PORT']
        
        for var in required_vars:
            if not getattr(cls, var):
                print(f"Error: {var} is not set")
                return False
        
        return True
    
    @classmethod
    def print_config(cls):
        """Print current configuration (without sensitive data)."""
        print("Current Configuration:")
        print(f"  DB_HOST: {cls.DB_HOST}")
        print(f"  DB_NAME: {cls.DB_NAME}")
        print(f"  DB_USER: {cls.DB_USER}")
        print(f"  DB_PASSWORD: {'*' * len(cls.DB_PASSWORD) if cls.DB_PASSWORD else 'Not set'}")
        print(f"  DB_PORT: {cls.DB_PORT}")
        print(f"  LOG_LEVEL: {cls.LOG_LEVEL}")


def create_env_file():
    """Create a sample .env file if it doesn't exist."""
    env_content = """# Database Configuration
# Update these values according to your PostgreSQL setup

DB_HOST=localhost
DB_NAME=streaming_analytics
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_PORT=5432

# Optional: Logging level
LOG_LEVEL=INFO
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content)
        print("Created .env file with default configuration")
        print("Please update the values in .env file according to your setup")
    else:
        print(".env file already exists")


if __name__ == "__main__":
    # Create .env file if it doesn't exist
    create_env_file()
    
    # Validate configuration
    if Config.validate_config():
        print("Configuration is valid")
        Config.print_config()
    else:
        print("Configuration is invalid. Please check your .env file or environment variables.")
