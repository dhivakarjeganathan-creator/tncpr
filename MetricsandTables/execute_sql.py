#!/usr/bin/env python3
"""
Script to execute metricsandtables.sql file in PostgreSQL database.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

def load_database_config():
    """Load database configuration from .env file."""
    load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'hierarchy_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'test@1234')
    }
    
    return config

def execute_sql_file(sql_file_path, db_config):
    """Execute SQL file against PostgreSQL database."""
    try:
        # Connect to PostgreSQL database
        print(f"Connecting to database: {db_config['database']} on {db_config['host']}:{db_config['port']}")
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Read SQL file
        print(f"Reading SQL file: {sql_file_path}")
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        # Execute SQL content
        print("Executing SQL commands...")
        cursor.execute(sql_content)
        
        # Commit the transaction
        connection.commit()
        print("SQL execution completed successfully!")
        
        # Verify the results by checking the metricsandtables table
        cursor.execute("SELECT COUNT(*) FROM metricsandtables;")
        count = cursor.fetchone()[0]
        print(f"Records inserted in metricsandtables table: {count}")
        
        # Show sample records
        cursor.execute("SELECT * FROM metricsandtables LIMIT 5;")
        sample_records = cursor.fetchall()
        if sample_records:
            print("\nSample records from metricsandtables:")
            for record in sample_records:
                print(f"  Table: {record[1]}, Metric: {record[0]}")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"SQL file not found: {sql_file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            print("Database connection closed.")

def main():
    """Main function to execute the SQL file."""
    print("=== PostgreSQL SQL Execution Script ===")
    
    # Load database configuration
    db_config = load_database_config()
    
    # SQL file path
    sql_file_path = "metricsandtables.sql"
    
    # Check if SQL file exists
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file '{sql_file_path}' not found in current directory.")
        sys.exit(1)
    
    # Execute SQL file
    execute_sql_file(sql_file_path, db_config)

if __name__ == "__main__":
    main()
