"""
Main ETL script for loading analytics data from CSV files to PostgreSQL.
"""
import os
import sys
import logging
from typing import Dict, Any
import pandas as pd

from database_config import DatabaseConfig
from table_schemas import TableSchemas
from csv_parser import CSVParser
from crud_operations import CRUDOperations
from dynamic_schema_generator import DynamicSchemaGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AnalyticsETL:
    """Main ETL class for analytics data processing."""
    
    def __init__(self, data_directory: str = "./output", config_file: str = "db_config.env"):
        """Initialize ETL process."""
        self.data_directory = data_directory
        self.db_config = DatabaseConfig(config_file)
        self.csv_parser = CSVParser(data_directory)
        self.crud_ops = CRUDOperations(self.db_config)
        self.schema_generator = DynamicSchemaGenerator(data_directory)
        
    def setup_database(self) -> bool:
        """Setup database tables."""
        try:
            logger.info("Setting up database...")
            
            # Test database connection
            if not self.db_config.test_connection():
                logger.error("Database connection failed")
                return False
            
            # Don't create tables here - create them dynamically as files are processed
            logger.info("Database connection established - tables will be created dynamically")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            return False
    
    def process_csv_files(self) -> Dict[str, pd.DataFrame]:
        """Process all CSV files."""
        try:
            logger.info("Processing CSV files...")
            
            # Get all CSV files
            csv_files = self.csv_parser.get_csv_files()
            logger.info(f"CSV files: {csv_files}")
            if not csv_files:
                logger.warning("No CSV files found")
                return "No CSV files found"
            
            logger.info(f"Found {len(csv_files)} CSV files to process")
            
            # Process each file
            processed_data = {}
            created_tables = set()
            
            for filename, table_name in csv_files:
                logger.info(f"Processing file: {filename} -> {table_name}")
                
                filepath = os.path.join(self.data_directory, filename)
                logger.info(f"Created_tables: {created_tables}")
                # Create table schema for this specific file if not already created
                if table_name not in created_tables:
                    logger.info(f"Creating table schema for {table_name} based on {filename}")
                    if not self._create_table_for_file(filepath, table_name):
                        logger.error(f"Failed to create table for {table_name}")
                        continue
                    created_tables.add(table_name)
                
                logger.info(f"Parser: parsing csv file")
                df = self.csv_parser.parse_csv_file(filepath, table_name)
                logger.info(f"Parser2: parsing csv file")
                if not df.empty:
                    if table_name in processed_data:
                        # Concatenate with existing data
                        processed_data[table_name] = pd.concat([processed_data[table_name], df], ignore_index=True)
                    else:
                        processed_data[table_name] = df
                    
                    logger.info(f"Processed {len(df)} rows from {filename}")
                else:
                    logger.warning(f"No valid data found in {filename}")
            
            logger.info(f"Processed {len(processed_data)} tables")
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing CSV files: {e}")
            return {}
    
    def _create_table_for_file(self, filepath: str, table_name: str) -> bool:
        """Create table schema for a specific file."""
        try:
            # Analyze the specific file
            df_header = pd.read_csv(filepath, nrows=0)
            df_sample = pd.read_csv(filepath, nrows=100)
            
            # Clean column names using the same logic as CSV parser
            clean_columns = {}
            for col in df_header.columns:
                clean_col = self.csv_parser.clean_column_name(col)
                clean_columns[col] = clean_col
            
            # Determine column types
            column_types = {}
            for col in df_sample.columns:
                clean_col = clean_columns[col]
                col_type = self.schema_generator.get_column_type(df_sample[col])
                column_types[clean_col] = col_type
            
            # Generate and execute CREATE TABLE SQL
            sql = self.schema_generator.generate_create_table_sql(table_name, column_types)
            self.db_config.execute_query(sql)
            logger.info(f"Created table: {table_name} with {len(column_types)} columns")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating table for {filepath}: {e}")
            return False
    
    def load_data_to_database(self, processed_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Load processed data to database."""
        try:
            logger.info("Loading data to database...")
            
            # Map table names to actual database table names (lowercase)
            table_name_mapping = {
                'MKT_Samsung': 'mkt_samsung',
                'DU_Samsung': 'du_samsung',
                'SECTOR_Samsung': 'sector_samsung',
                'CARRIER_Samsung': 'carrier_samsung',
                'MKT_Corning': 'mkt_corning',
                'GNB_Corning': 'gnb_corning',                
                'DU_Corning': 'du_corning',
                'SECTOR_Corning': 'sector_corning',
                'CARRIER_Corning': 'carrier_corning',
                'MKT_Ericsson': 'mkt_ericsson',
                'GNB_Ericsson': 'gnb_ericsson',
                'SECTOR_Ericsson': 'sector_ericsson',
                'CARRIER_Ericsson': 'carrier_ericsson',
                'ACPF_GNB_Samsung': 'acpf_gnb_samsung',  
                'ACPF_VCU_Samsung': 'acpf_vcu_samsung',  
                'AUPF_GNB_Samsung': 'aupf_gnb_samsung',  
                'AUPF_VCU_Samsung': 'aupf_vcu_samsung',  
                'AUPF_VM_Samsung': 'aupf_vm_samsung',  
            }
            
            load_results = {}
            
            for table_name, df in processed_data.items():
                if df.empty:
                    logger.warning(f"No data to load for table: {table_name}")
                    continue
                
                # Get the actual database table name
                db_table_name = table_name_mapping.get(table_name, table_name.lower())
                logger.info(f"Loading {len(df)} rows to table: {db_table_name}")
                
                # Bulk insert data
                rows_inserted = self.crud_ops.bulk_insert_from_dataframe(db_table_name, df)
                load_results[table_name] = rows_inserted
                
                logger.info(f"Inserted {rows_inserted} rows into {db_table_name}")
            
            return load_results
            
        except Exception as e:
            logger.error(f"Error loading data to database: {e}")
            return {}
    
    def validate_data_loading(self, load_results: Dict[str, int]) -> bool:
        """Validate data loading results."""
        try:
            logger.info("Validating data loading...")
            
            total_inserted = sum(load_results.values())
            logger.info(f"Total rows inserted: {total_inserted}")
            
            # Get table row counts
            table_names = TableSchemas.get_all_table_names()
            for table_name in table_names:
                row_count = self.crud_ops.get_table_row_count(table_name)
                logger.info(f"Table {table_name}: {row_count} rows")
            
            return total_inserted > 0
            
        except Exception as e:
            logger.error(f"Error validating data loading: {e}")
            return False
    
    def run_etl(self) -> bool:
        """Run complete ETL process."""
        try:
            logger.info("Starting ETL process...")
            logger.info("1")
            # Step 1: Setup database
            if not self.setup_database():
                logger.error("Database setup failed")
                return False
            # Step 2: Process CSV files
            processed_data = self.process_csv_files()
            if not processed_data:
                logger.info("No data processed from CSV files")
                return False
            elif processed_data == "No CSV files found":
                logger.info("No CSV files found")
                return True
            # Step 3: Load data to database
            load_results = self.load_data_to_database(processed_data)
            if not load_results:
                logger.error("No data loaded to database")
                return False
            # Step 4: Validate loading
            # if not self.validate_data_loading(load_results):
            #     logger.error("Data loading validation failed")
            #     return False
            logger.info("ETL process completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in ETL process: {e}")
            return False
        finally:
            # Close database connection
            self.db_config.close_connection()
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get database summary information."""
        try:
            tables_info = self.crud_ops.get_all_tables_info()
            
            summary = {
                'total_tables': len(tables_info),
                'tables': {}
            }
            
            total_rows = 0
            for table_name, info in tables_info.items():
                row_count = info.get('row_count', 0)
                total_rows += row_count
                
                summary['tables'][table_name] = {
                    'row_count': row_count,
                    'column_count': len(info.get('columns', []))
                }
            
            summary['total_rows'] = total_rows
            return summary
            
        except Exception as e:
            logger.error(f"Error getting database summary: {e}")
            return {}

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analytics ETL Process')
    parser.add_argument('--data-dir', default='./output', help='Data directory containing CSV files')
    parser.add_argument('--config', default='db_config.env', help='Database configuration file')
    parser.add_argument('--setup-only', action='store_true', help='Only setup database tables')
    parser.add_argument('--summary', action='store_true', help='Show database summary')
    
    args = parser.parse_args()
    
    # Initialize ETL
    etl = AnalyticsETL(args.data_dir, args.config)
    
    try:
        if args.setup_only:
            # Only setup database
            if etl.setup_database():
                logger.info("Database setup completed successfully")
            else:
                logger.error("Database setup failed")
                sys.exit(1)
        
        elif args.summary:
            # Show database summary
            if etl.setup_database():
                summary = etl.get_database_summary()
                print("\nDatabase Summary:")
                print(f"Total Tables: {summary['total_tables']}")
                print(f"Total Rows: {summary['total_rows']}")
                print("\nTable Details:")
                for table_name, info in summary['tables'].items():
                    print(f"  {table_name}: {info['row_count']} rows, {info['column_count']} columns")
            else:
                logger.error("Failed to connect to database")
                sys.exit(1)
        
        else:
            # Run complete ETL process
            if etl.run_etl():
                logger.info("ETL process completed successfully")
                
                # # Show summary
                # summary = etl.get_database_summary()
                # logger.info(f"Summary: {summary}")
                # print("\nETL Summary:")
                # print(f"Total Tables: {summary['total_tables']}")
                # print(f"Total Rows: {summary['total_rows']}")
            else:
                logger.error("ETL process failed")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("ETL process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
