#!/usr/bin/env python3
"""
JSON Data Loader Application
This application loads JSON data from Group_configuration.json and Time_scheduling.json
into a PostgreSQL database.
"""

import os
import sys
from database import create_tables, test_connection
from data_loader import load_all_data, get_data_summary

def print_banner():
    """Print application banner"""
    print("=" * 60)
    print("JSON Data Loader Application")
    print("=" * 60)
    print("This application will load JSON and CSV data into PostgreSQL database")
    print("=" * 60)

def check_files():
    """Check if required JSON and CSV files exist"""
    required_files = [
        'Group_configuration.json', 
        'Time_scheduling.json',
        'Ericsson-5G-Enrichment.csv',
        'Samsung-5G-Enrichment.csv'
    ]
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"ERROR: Missing required files: {', '.join(missing_files)}")
        return False
    
    print("SUCCESS: All required files found")
    return True

def main():
    """Main application function"""
    print_banner()
    
    # Check if required files exist
    if not check_files():
        print("\nERROR: Please ensure all required JSON files are present in the current directory")
        sys.exit(1)
    
    # Check database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("\nERROR: Database connection failed!")
        print("Please check your database configuration in the .env file")
        print("\nRequired configuration:")
        print("- DB_HOST: PostgreSQL server host")
        print("- DB_PORT: PostgreSQL server port (default: 5432)")
        print("- DB_NAME: Database name")
        print("- DB_USER: Database username")
        print("- DB_PASSWORD: Database password")
        sys.exit(1)
    
    # Create database tables
    print("\nCreating database tables...")
    if not create_tables():
        print("\nERROR: Failed to create database tables!")
        sys.exit(1)
    
    # Load data
    print("\nLoading data from JSON files...")
    if load_all_data():
        print("\nSUCCESS: Data loading completed successfully!")
        get_data_summary()
    else:
        print("\nERROR: Data loading failed!")
        sys.exit(1)
    
    print("\nApplication completed successfully!")
    print("\nYou can now query your data using any PostgreSQL client or the provided scripts.")

if __name__ == "__main__":
    main()
