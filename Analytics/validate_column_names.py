"""
Validation script to check if column names of dynamically created tables 
match the column names in CSV files.
"""
import os
import pandas as pd
import logging
from typing import Dict, List, Tuple, Set
from database_config import DatabaseConfig
from csv_parser import CSVParser
from dynamic_schema_generator import DynamicSchemaGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('column_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ColumnNameValidator:
    """Validates that database table column names match CSV file column names."""
    
    def __init__(self, data_directory: str = ".", config_file: str = "db_config.env"):
        """Initialize validator."""
        self.data_directory = data_directory
        self.db_config = DatabaseConfig(config_file)
        self.csv_parser = CSVParser(data_directory)
        self.schema_generator = DynamicSchemaGenerator(data_directory)
        
    def get_csv_column_names(self, filepath: str) -> Tuple[List[str], List[str]]:
        """Get original and cleaned column names from CSV file."""
        try:
            # Read just the header
            df_header = pd.read_csv(filepath, nrows=0)
            original_columns = list(df_header.columns)
            
            # Clean column names using the same logic as CSV parser
            cleaned_columns = []
            for col in original_columns:
                clean_col = self.csv_parser.clean_column_name(col)
                cleaned_columns.append(clean_col)
            
            return original_columns, cleaned_columns
            
        except Exception as e:
            logger.error(f"Error reading CSV columns from {filepath}: {e}")
            return [], []
    
    def get_database_column_names(self, table_name: str) -> List[str]:
        """Get column names from database table."""
        try:
            sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name NOT IN ('created_at', 'updated_at')
            ORDER BY ordinal_position
            """
            
            result = self.db_config.execute_query(sql, (table_name,))
            return [row['column_name'] for row in result]
            
        except Exception as e:
            logger.error(f"Error getting database columns for {table_name}: {e}")
            return []
    
    def compare_columns(self, csv_columns: List[str], db_columns: List[str], 
                       filepath: str, table_name: str) -> Dict[str, any]:
        """Compare CSV and database column names."""
        csv_set = set(csv_columns)
        db_set = set(db_columns)
        
        # Find differences
        missing_in_db = csv_set - db_set
        extra_in_db = db_set - csv_set
        matching = csv_set & db_set
        
        result = {
            'filepath': filepath,
            'table_name': table_name,
            'csv_columns': csv_columns,
            'db_columns': db_columns,
            'matching_columns': list(matching),
            'missing_in_db': list(missing_in_db),
            'extra_in_db': list(extra_in_db),
            'is_match': len(missing_in_db) == 0 and len(extra_in_db) == 0,
            'csv_count': len(csv_columns),
            'db_count': len(db_columns)
        }
        
        return result
    
    def validate_all_files(self) -> Dict[str, Dict[str, any]]:
        """Validate column names for all CSV files."""
        logger.info("Starting column name validation...")
        
        # Get all CSV files
        csv_files = self.csv_parser.get_csv_files()
        if not csv_files:
            logger.warning("No CSV files found")
            return {}
        
        validation_results = {}
        
        for filename, table_name in csv_files:
            filepath = os.path.join(self.data_directory, filename)
            logger.info(f"Validating: {filename} -> {table_name}")
            
            # Get CSV column names
            original_columns, cleaned_columns = self.get_csv_column_names(filepath)
            if not cleaned_columns:
                logger.error(f"Could not read columns from {filename}")
                continue
            
            # Get database column names
            db_columns = self.get_database_column_names(table_name)
            if not db_columns:
                logger.error(f"Could not read columns from table {table_name}")
                continue
            
            # Compare columns
            result = self.compare_columns(cleaned_columns, db_columns, filepath, table_name)
            validation_results[filename] = result
            
            # Log results
            if result['is_match']:
                logger.info(f"✓ {filename}: Column names match perfectly")
            else:
                logger.warning(f"✗ {filename}: Column mismatch detected")
                if result['missing_in_db']:
                    logger.warning(f"  Missing in DB: {result['missing_in_db']}")
                if result['extra_in_db']:
                    logger.warning(f"  Extra in DB: {result['extra_in_db']}")
        
        return validation_results
    
    def print_validation_summary(self, validation_results: Dict[str, Dict[str, any]]):
        """Print a summary of validation results."""
        if not validation_results:
            print("No validation results to display")
            return
        
        print("\n" + "="*80)
        print("COLUMN NAME VALIDATION SUMMARY")
        print("="*80)
        
        total_files = len(validation_results)
        matching_files = sum(1 for result in validation_results.values() if result['is_match'])
        mismatched_files = total_files - matching_files
        
        print(f"Total files processed: {total_files}")
        print(f"Files with matching columns: {matching_files}")
        print(f"Files with mismatched columns: {mismatched_files}")
        print(f"Success rate: {(matching_files/total_files)*100:.1f}%")
        
        if mismatched_files > 0:
            print("\n" + "-"*60)
            print("DETAILED MISMATCH REPORT")
            print("-"*60)
            
            for filename, result in validation_results.items():
                if not result['is_match']:
                    print(f"\nFile: {filename}")
                    print(f"Table: {result['table_name']}")
                    print(f"CSV columns ({result['csv_count']}): {result['csv_columns']}")
                    print(f"DB columns ({result['db_count']}): {result['db_columns']}")
                    
                    if result['missing_in_db']:
                        print(f"Missing in DB: {result['missing_in_db']}")
                    if result['extra_in_db']:
                        print(f"Extra in DB: {result['extra_in_db']}")
        
        print("\n" + "="*80)
    
    def generate_column_mapping_report(self, validation_results: Dict[str, Dict[str, any]]):
        """Generate a detailed column mapping report."""
        report_file = "column_mapping_report.txt"
        
        try:
            with open(report_file, 'w') as f:
                f.write("COLUMN NAME MAPPING REPORT\n")
                f.write("="*50 + "\n\n")
                
                for filename, result in validation_results.items():
                    f.write(f"File: {filename}\n")
                    f.write(f"Table: {result['table_name']}\n")
                    f.write(f"Status: {'✓ MATCH' if result['is_match'] else '✗ MISMATCH'}\n")
                    f.write("-" * 40 + "\n")
                    
                    # Show original -> cleaned -> database mapping
                    f.write("Column Mapping:\n")
                    for i, orig_col in enumerate(result['csv_columns']):
                        cleaned_col = result['csv_columns'][i]  # This is already cleaned
                        db_exists = cleaned_col in result['db_columns']
                        status = "✓" if db_exists else "✗"
                        f.write(f"  {status} '{orig_col}' -> '{cleaned_col}' -> {'EXISTS' if db_exists else 'MISSING'}\n")
                    
                    f.write("\n")
            
            logger.info(f"Column mapping report saved to: {report_file}")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
    
    def run_validation(self) -> bool:
        """Run complete validation process."""
        try:
            # Test database connection
            if not self.db_config.test_connection():
                logger.error("Database connection failed")
                return False
            
            # Run validation
            validation_results = self.validate_all_files()
            
            if not validation_results:
                logger.warning("No files were validated")
                return False
            
            # Print summary
            self.print_validation_summary(validation_results)
            
            # Generate report
            self.generate_column_mapping_report(validation_results)
            
            # Check if all files match
            all_match = all(result['is_match'] for result in validation_results.values())
            
            if all_match:
                logger.info("All column names match perfectly!")
                return True
            else:
                logger.warning("Some column names do not match - check the report for details")
                return False
                
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            return False
        finally:
            self.db_config.close_connection()

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate CSV and Database Column Names')
    parser.add_argument('--data-dir', default='.', help='Data directory containing CSV files')
    parser.add_argument('--config', default='db_config.env', help='Database configuration file')
    
    args = parser.parse_args()
    
    # Initialize validator
    validator = ColumnNameValidator(args.data_dir, args.config)
    
    # Run validation
    success = validator.run_validation()
    
    if success:
        print("\n✓ Validation completed successfully - all column names match!")
        exit(0)
    else:
        print("\n✗ Validation found mismatches - check the logs and report for details")
        exit(1)

if __name__ == "__main__":
    main()

