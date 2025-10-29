"""
Configuration file for the Hierarchy GNB Processor

This module loads configuration from environment variables with fallback defaults.
Create a .env file in the project root to override default values.

Example .env file:
DB_HOST=localhost
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_PORT=5432
EXCEL_FILE_PATH=AUPF_ACPF_GNB_Hierarchy.xlsx
CLEAR_EXISTING_DATA=True
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'hierarchy_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'test@1234'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# File Configuration
EXCEL_FILE_PATH = os.getenv('EXCEL_FILE_PATH', 'AUPF_ACPF_GNB_Hierarchy.xlsx')

# Processing Configuration
CLEAR_EXISTING_DATA = os.getenv('CLEAR_EXISTING_DATA', 'True').lower() == 'true'
