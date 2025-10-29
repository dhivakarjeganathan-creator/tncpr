"""
Configuration settings for Batch Processing Algorithm
"""

import os
from typing import Dict, Any

class Config:
    """Configuration class for batch processing"""
    
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/analytics_db')
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Batch processing configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '60'))  # seconds
    
    # Scheduler configuration
    SCHEDULER_CHECK_INTERVAL = int(os.getenv('SCHEDULER_CHECK_INTERVAL', '60'))  # seconds
    
    # Event name to cron mapping
    EVENT_CRON_MAPPING = {
        "EVERYDAY8AMET": "0 8 * * *",
        "EVERYHOUR": "0 * * * *",
        "EVERYWEEK": "0 0 * * 0",
        "EVERYHOURBYMIN10": "10 * * * *",
        "EVERYDAY": "0 0 * * *",
        "EVERYMINUTE": "* * * * *",
        "EVERY5MINUTES": "*/5 * * * *",
        "EVERY15MINUTES": "*/15 * * * *",
        "EVERY30MINUTES": "*/30 * * * *",
    }
    
    # Granularity mapping
    GRANULARITY_HOURS = {
        "1-hour": 1,
        "2-hour": 2,
        "4-hour": 4,
        "6-hour": 6,
        "12-hour": 12,
        "1-day": 24,
        "2-day": 48,
        "3-day": 72,
        "1-week": 168,
        "2-week": 336,
    }
    
    # Aggregation types
    SUPPORTED_AGGREGATIONS = [
        "sum", "avg", "count", "min", "max", 
        "median", "std", "var", "first", "last"
    ]
    
    # Table configurations
    RULE_EXECUTION_RESULTS_TABLE = "RuleExecutionResults"
    
    # Performance settings
    MAX_CONCURRENT_JOBS = int(os.getenv('MAX_CONCURRENT_JOBS', '10'))
    JOB_TIMEOUT = int(os.getenv('JOB_TIMEOUT', '3600'))  # seconds
    
    @classmethod
    def get_database_config(cls) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            'url': cls.DATABASE_URL,
            'pool_size': 10,
            'max_overflow': 20,
            'pool_timeout': 30,
            'pool_recycle': 3600
        }
    
    @classmethod
    def get_logging_config(cls) -> Dict[str, Any]:
        """Get logging configuration"""
        return {
            'level': cls.LOG_LEVEL,
            'format': cls.LOG_FORMAT,
            'handlers': ['console', 'file'],
            'file_path': 'batch_processor.log'
        }
