#!/usr/bin/env python3
"""
Database setup script for the Hierarchy GNB Processor

This script creates the database and user if they don't exist.
Run this script as a PostgreSQL superuser before running the main processor.
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def create_database_and_user():
    """Create database and user for the hierarchy processor"""
    
    # Import configuration
    from config import DATABASE_CONFIG
    
    # Connection parameters for the default PostgreSQL database
    # Use the same host, port, and credentials from config
    admin_config = {
        'host': DATABASE_CONFIG['host'],
        'database': 'postgres',  # Connect to default postgres database
        'user': DATABASE_CONFIG['user'],
        'password': DATABASE_CONFIG['password'],
        'port': DATABASE_CONFIG['port']
    }
    
    try:
        # Connect as superuser
        connection = psycopg2.connect(**admin_config)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with connection.cursor() as cursor:
            # Create database
            cursor.execute("""
                SELECT 1 FROM pg_database WHERE datname = 'hierarchy_db'
            """)
            if not cursor.fetchone():
                cursor.execute("CREATE DATABASE hierarchy_db")
                print("Database 'hierarchy_db' created successfully")
            else:
                print("Database 'hierarchy_db' already exists")
            
            # Create user (optional - you can use existing user)
            cursor.execute("""
                SELECT 1 FROM pg_roles WHERE rolname = 'hierarchy_user'
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    CREATE USER hierarchy_user WITH PASSWORD 'hierarchy_password'
                """)
                cursor.execute("""
                    GRANT ALL PRIVILEGES ON DATABASE hierarchy_db TO hierarchy_user
                """)
                print("User 'hierarchy_user' created successfully")
            else:
                print("User 'hierarchy_user' already exists")
        
        connection.close()
        print("Database setup completed successfully!")
        
    except psycopg2.Error as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("Setting up database for Hierarchy GNB Processor...")
    create_database_and_user()
