"""
Configuration management for the threshold rules processor.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for database and application settings."""
    
    # Database configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'threshold_rules')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    
    # Application configuration
    JSON_FILE = os.getenv('JSON_FILE', 'Threshold_definitions.json')
    
    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Get database configuration as dictionary."""
        return {
            'host': cls.DB_HOST,
            'port': cls.DB_PORT,
            'database': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD
        }
    
    @classmethod
    def validate_db_config(cls) -> bool:
        """Validate that all required database configuration is present."""
        required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not getattr(cls, var)]
        
        if missing_vars:
            print(f"Missing required environment variables: {', '.join(missing_vars)}")
            return False
        
        return True

def create_env_template():
    """Create a .env template file."""
    env_template = """# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=threshold_rules
DB_USER=postgres
DB_PASSWORD=your_password_here

# Application Configuration
JSON_FILE=Threshold_definitions.json
"""
    
    with open('.env.template', 'w') as f:
        f.write(env_template)
    
    print("Created .env.template file. Copy it to .env and update with your values.")
