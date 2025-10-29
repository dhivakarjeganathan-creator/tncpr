"""
Example usage of the Analytics ETL System.
"""
import os
import sys
import logging
from database_config import DatabaseConfig
from csv_parser import CSVParser
from crud_operations import CRUDOperations
from table_schemas import TableSchemas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def example_basic_usage():
    """Example of basic system usage."""
    print("Analytics ETL System - Basic Usage Example")
    print("=" * 50)
    
    # Initialize components
    db_config = DatabaseConfig()
    csv_parser = CSVParser(".")
    crud_ops = CRUDOperations(db_config)
    
    try:
        # Step 1: Setup database
        print("1. Setting up database...")
        if not crud_ops.create_tables():
            print("   Failed to create tables")
            return False
        print("   Database tables created successfully")
        
        # Step 2: Process CSV files
        print("\n2. Processing CSV files...")
        csv_files = csv_parser.get_csv_files()
        print(f"   Found {len(csv_files)} CSV files")
        
        for filename, table_name in csv_files:
            print(f"   Processing: {filename} -> {table_name}")
            
            # Parse CSV file
            df = csv_parser.parse_csv_file(filename, table_name)
            if not df.empty:
                print(f"     Loaded {len(df)} rows")
                
                # Insert data into database
                rows_inserted = crud_ops.bulk_insert_from_dataframe(table_name, df)
                print(f"     Inserted {rows_inserted} rows into database")
            else:
                print(f"     No valid data found")
        
        # Step 3: Show summary
        print("\n3. Database Summary:")
        tables_info = crud_ops.get_all_tables_info()
        
        total_rows = 0
        for table_name, info in tables_info.items():
            row_count = info.get('row_count', 0)
            total_rows += row_count
            print(f"   {table_name}: {row_count} rows")
        
        print(f"\n   Total rows across all tables: {total_rows}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        db_config.close_connection()

def example_crud_operations():
    """Example of CRUD operations."""
    print("\nAnalytics ETL System - CRUD Operations Example")
    print("=" * 50)
    
    db_config = DatabaseConfig()
    crud_ops = CRUDOperations(db_config)
    
    try:
        # Example: Query data
        print("1. Querying data from MKT_Samsung table...")
        results = crud_ops.select_data(
            table_name="MKT_Samsung",
            columns=["market", "region", "timestamp"],
            limit=5
        )
        
        print(f"   Found {len(results)} records")
        for i, record in enumerate(results[:3]):  # Show first 3
            print(f"   Record {i+1}: {record}")
        
        # Example: Get table information
        print("\n2. Getting table information...")
        table_info = crud_ops.get_table_info("MKT_Samsung")
        print(f"   Table: {table_info.get('table_name', 'Unknown')}")
        print(f"   Rows: {table_info.get('row_count', 0)}")
        print(f"   Columns: {len(table_info.get('columns', []))}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        db_config.close_connection()

def example_data_validation():
    """Example of data validation."""
    print("\nAnalytics ETL System - Data Validation Example")
    print("=" * 50)
    
    csv_parser = CSVParser(".")
    
    try:
        # Get CSV files
        csv_files = csv_parser.get_csv_files()
        
        if not csv_files:
            print("No CSV files found")
            return False
        
        # Process first file as example
        filename, table_name = csv_files[0]
        print(f"Processing file: {filename}")
        print(f"Table: {table_name}")
        
        # Parse file
        df = csv_parser.parse_csv_file(filename, table_name)
        
        if df.empty:
            print("No data found in file")
            return False
        
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
        
        # Show data types
        print("\nData types:")
        for col, dtype in df.dtypes.items():
            print(f"  {col}: {dtype}")
        
        # Show sample data
        print(f"\nSample data (first 3 rows):")
        print(df.head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    """Run all examples."""
    print("Analytics ETL System - Usage Examples")
    print("=" * 60)
    
    examples = [
        ("Basic Usage", example_basic_usage),
        ("CRUD Operations", example_crud_operations),
        ("Data Validation", example_data_validation)
    ]
    
    for example_name, example_func in examples:
        try:
            print(f"\n{example_name}:")
            print("-" * len(example_name))
            success = example_func()
            print(f"Result: {'SUCCESS' if success else 'FAILED'}")
        except Exception as e:
            print(f"Result: ERROR - {e}")
        
        print()

if __name__ == "__main__":
    main()
