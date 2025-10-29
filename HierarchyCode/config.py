"""
Configuration settings for the Hierarchy Management System
"""
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Application configuration"""
    
    # Database configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'hierarchy_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'test@1234')
    
    # CSV processing configuration
    CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH', './csv_files')
    
    # Supported CSV column names (case-insensitive)
    COLUMN_MAPPINGS = {
        'region': ['region', 'REGION'],
        'market': ['market', 'MARKET'],
        'gnb': ['gnb', 'GNB'],
        'du': ['du', 'DU'],
        'sector': ['sector', 'SECTOR'],
        'carrier': ['carrier', 'CARRIER']
    }
    
    # Database connection string
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{quote_plus(self.DB_PASSWORD)}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

# Create global config instance
config = Config()
