"""
Script to reset the database by dropping and recreating all tables.
"""
import sys
from database_config import DatabaseConfig
from crud_operations import CRUDOperations

def main():
    """Reset database by dropping and recreating all tables."""
    print("Resetting database...")
    
    # Initialize components
    db_config = DatabaseConfig()
    crud_ops = CRUDOperations(db_config)
    
    try:
        # Drop all tables
        print("Dropping existing tables...")
        if crud_ops.drop_tables():
            print("Tables dropped successfully")
        else:
            print("Error dropping tables")
            return False
        
        # Create tables with new schema
        print("Creating tables with updated schema...")
        if crud_ops.create_tables():
            print("Tables created successfully")
            return True
        else:
            print("Error creating tables")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        db_config.close_connection()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
