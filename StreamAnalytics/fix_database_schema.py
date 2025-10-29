"""
Database Schema Fix Script
This script fixes the database schema by adding missing unique constraints.
"""

import psycopg2
from config import Config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_database_schema():
    """Fix the database schema by adding missing constraints."""
    try:
        # Connect to database
        conn = psycopg2.connect(**Config.get_db_config())
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            # Check if the unique constraint already exists
            cursor.execute("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'job_definitions' 
                AND constraint_type = 'UNIQUE' 
                AND constraint_name LIKE '%job_id%'
            """)
            
            existing_constraints = cursor.fetchall()
            
            if not existing_constraints:
                logger.info("Adding unique constraint to job_definitions.job_id")
                cursor.execute("""
                    ALTER TABLE job_definitions 
                    ADD CONSTRAINT job_definitions_job_id_unique UNIQUE (job_id)
                """)
                logger.info("✓ Unique constraint added successfully")
            else:
                logger.info("✓ Unique constraint already exists")
            
            # Commit the changes
            conn.commit()
            logger.info("Database schema fixed successfully")
            
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def main():
    """Main function to fix the database schema."""
    print("Fixing Database Schema")
    print("=" * 30)
    
    try:
        fix_database_schema()
        print("\n✓ Database schema fixed successfully!")
        print("You can now run the streaming analytics loader again.")
    except Exception as e:
        print(f"\n✗ Failed to fix database schema: {e}")
        print("Please check your database connection and try again.")

if __name__ == "__main__":
    main()
