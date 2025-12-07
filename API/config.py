"""
Configuration module for KPI Timeseries API
Supports both PostgreSQL and Watsonx Data (Presto) connections
"""
import os
import logging
from typing import Optional
from enum import Enum
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file if it exists
# Handle errors gracefully if .env file has encoding issues or doesn't exist
try:
    load_dotenv()
except (UnicodeDecodeError, IOError, Exception) as e:
    # If .env file doesn't exist or has encoding issues, continue with defaults
    logger.warning(f"Could not load .env file: {e}. Using default configuration and environment variables.")


class DatabaseType(str, Enum):
    """Database type enumeration"""
    POSTGRESQL = "postgresql"
    PRESTO = "presto"


class Config:
    """Application configuration"""
    
    # Database Configuration
    DB_TYPE: str = os.getenv("DB_TYPE", DatabaseType.POSTGRESQL.value)
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "hierarchy_db")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "test@1234")
    
    # Presto/Watsonx specific configuration
    PRESTO_CATALOG: Optional[str] = os.getenv("PRESTO_CATALOG", None)
    PRESTO_SCHEMA: Optional[str] = os.getenv("PRESTO_SCHEMA", None)
    
    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Table Configuration
    # Entity columns for MKT_Corning
    MKT_CORNING_ENTITY_COLUMNS = ["market", "region", "vcptype", "technology", "datacenter", "site", "id"]
    MKT_CORNING_TIMESTAMP_COLUMN = "timestamp"
    MKT_CORNING_METADATA_COLUMNS = ["createddate", "updateddate"]
    
    @classmethod
    def get_connection_string(cls) -> str:
        """Get database connection string based on DB type"""
        if cls.DB_TYPE == DatabaseType.POSTGRESQL.value:
            return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        elif cls.DB_TYPE == DatabaseType.PRESTO.value:
            # Presto connection string format
            catalog = cls.PRESTO_CATALOG or "hive"
            return f"presto://{cls.DB_USER}@{cls.DB_HOST}:{cls.DB_PORT}/{catalog}/{cls.PRESTO_SCHEMA or cls.DB_NAME}"
        else:
            raise ValueError(f"Unsupported database type: {cls.DB_TYPE}")

