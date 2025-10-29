"""
Dynamic schema generator that creates database tables based on actual CSV file columns.
"""
import pandas as pd
import os
import re
from typing import Dict, List, Set
from database_config import DatabaseConfig
import logging

logger = logging.getLogger(__name__)

class DynamicSchemaGenerator:
    """Generate database schemas dynamically based on CSV file columns."""
    
    def __init__(self, data_directory: str = "."):
        """Initialize schema generator."""
        self.data_directory = data_directory
        self.db_config = DatabaseConfig()
        
        # File pattern mapping
        self.file_patterns = {
            '_MKT_LVL_': 'MKT_Samsung',  # Samsung Market files
            '_GNB_LVL_': 'GNB_Samsung',  # Samsung GNB files
            '_DU_LVL_': 'DU_Samsung',    # Samsung DU files
            '_SECTOR_LVL_': 'SECTOR_Samsung',  # Samsung Sector files
            '_CARRIER_LVL_': 'CARRIER_Samsung',  # Samsung Carrier files
            '_ACPF_VCU_LVL': 'ACPF_Samsung',  # Samsung VCU files
            '_VCU_LVL': 'AUPF_Samsung',  # Samsung VCU files
            '_VM_ETH_LVL': 'AUPF_Samsung_extn',  # Samsung VCU files
            'CORNING_MKT_LVL_': 'MKT_Corning',  # Corning Market files
            'CORNING_GNB_LVL_': 'GNB_Corning',  # Corning GNB files
            'CORNING_DU_LVL_': 'DU_Corning',  # Corning DU files
            'CORNING_SECTOR_LVL_': 'SECTOR_Corning',  # Corning Sector files
            'CORNING_CARRIER_LVL_': 'CARRIER_Corning',  # Corning Carrier files
            'ER_5G_MKT_LVL_': 'MKT_Ericsson',  # Ericsson Market files
            'ER_5G_GNB_LVL_': 'GNB_Ericsson',  # Ericsson GNB files
            'ER_5G_SECTOR_LVL_': 'SECTOR_Ericsson',  # Ericsson Sector files
            'ER_5G_CARRIER_LVL_': 'CARRIER_Ericsson'  # Ericsson Carrier files
        }
    
    def clean_column_name(self, col_name: str) -> str:
        """Clean column name for database compatibility using CSV parser logic."""
        # Remove quotes and special characters
        col_name = col_name.strip("'\"")
        
        # Replace special characters with underscores (same as CSV parser)
        col_name = re.sub(r'[^\w\s]', '_', col_name)
        
        # Replace multiple underscores with single underscore
        col_name = re.sub(r'_+', '_', col_name)
        
        # Remove leading/trailing underscores
        col_name = col_name.strip('_')
        
        # Convert to lowercase
        col_name = col_name.lower()
        
        return col_name
    
    def get_column_type(self, series: pd.Series) -> str:
        """Determine PostgreSQL column type based on pandas series."""
        # Check if all values are null
        if series.isna().all():
            return "TEXT"
        
        # Special handling for timestamp column - always treat as VARCHAR(50)
        if series.name and 'timestamp' in series.name.lower():
            return "VARCHAR(50)"
        
        # Special handling for Id column - always treat as VARCHAR(50)
        if series.name and series.name.lower() == 'id':
            return "VARCHAR(50)"
        
        # Special handling for market column - always treat as text
        if series.name and 'market' in series.name.lower():
            return "VARCHAR(50)"
        
        # Remove null values for type checking
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return "TEXT"
        
        # Check if it's numeric
        try:
            pd.to_numeric(non_null_series)
            # Check if it's integer AND doesn't have leading zeros
            if non_null_series.apply(lambda x: str(x).replace('.', '').replace('-', '').isdigit()).all():
                # Check if any value has leading zeros (starts with 0 but is not just "0")
                has_leading_zeros = non_null_series.astype(str).apply(
                    lambda x: x.startswith('0') and len(x) > 1 and x != '0'
                ).any()
                if has_leading_zeros:
                    # Keep as text to preserve leading zeros
                    return "VARCHAR(50)"
                else:
                    return "BIGINT"
            else:
                return "DECIMAL(15,6)"
        except (ValueError, TypeError):
            pass
        
        # Check string length
        max_length = non_null_series.astype(str).str.len().max()
        if max_length <= 50:
            return "VARCHAR(50)"
        elif max_length <= 100:
            return "VARCHAR(100)"
        elif max_length <= 255:
            return "VARCHAR(255)"
        else:
            return "TEXT"
    
    def analyze_csv_files(self) -> Dict[str, Dict[str, str]]:
        """Analyze all CSV files and determine their column schemas."""
        table_schemas = {}
        
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv') and not filename.startswith('User-defined'):
                # Identify table name
                table_name = self.identify_file_type(filename)
                if not table_name:
                    continue
                
                logger.info(f"Analyzing file: {filename} -> {table_name}")
                
                try:
                    # Read just the header to get column names
                    df_header = pd.read_csv(os.path.join(self.data_directory, filename), nrows=0)
                    
                    # Clean column names
                    clean_columns = {}
                    for col in df_header.columns:
                        clean_col = self.clean_column_name(col)
                        clean_columns[col] = clean_col
                    
                    # Read a sample of data to determine types
                    df_sample = pd.read_csv(os.path.join(self.data_directory, filename), nrows=100)
                    
                    # Determine column types
                    column_types = {}
                    for col in df_sample.columns:
                        clean_col = clean_columns[col]
                        col_type = self.get_column_type(df_sample[col])
                        column_types[clean_col] = col_type
                    
                    # Store schema for this table - use the first file's schema for each table type
                    if table_name not in table_schemas:
                        table_schemas[table_name] = column_types
                        logger.info(f"Created schema for {table_name} based on {filename}")
                    else:
                        logger.info(f"Skipping {filename} - schema already exists for {table_name}")
                    
                except Exception as e:
                    logger.error(f"Error analyzing file {filename}: {e}")
                    continue
        
        return table_schemas
    
    def identify_file_type(self, filename: str) -> str:
        """Identify file type based on filename patterns."""
        filename_upper = filename.upper()
        
        for pattern, table_name in self.file_patterns.items():
            if pattern in filename_upper:
                # Special handling for VCU files
                if pattern == '_VCU_LVL_':
                    if '13-' in filename:
                        return 'ACPF_Samsung'
                    elif '12-' in filename:
                        return 'AUPF_Samsung'
                    else:
                        return 'ACPF_Samsung'  # Default to ACPF
                return table_name
        
        return None
    
    def generate_create_table_sql(self, table_name: str, columns: Dict[str, str]) -> str:
        """Generate CREATE TABLE SQL for a table."""
        # Determine primary key columns - all tables now use id and timestamp
        primary_key_cols = ['id', 'timestamp']
        
        # Build column definitions
        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(f"    {col_name} {col_type}")
        
        # Add primary key constraint
        primary_key_sql = ", ".join(primary_key_cols)
        
        sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(column_defs)},
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ({primary_key_sql})
);
"""
        logger.info(f"CREATE TABLE SQL: {sql}")
        return sql
    
    def create_all_tables(self) -> bool:
        """Create all tables based on analyzed CSV files."""
        try:
            # Analyze CSV files
            table_schemas = self.analyze_csv_files()
            
            if not table_schemas:
                logger.error("No table schemas generated")
                return False
            
            # Create tables
            for table_name, columns in table_schemas.items():
                logger.info(f"Creating table: {table_name} with {len(columns)} columns")
                
                sql = self.generate_create_table_sql(table_name, columns)
                
                # Execute SQL
                self.db_config.execute_query(sql)
                logger.info(f"Created table: {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def drop_all_tables(self) -> bool:
        """Drop all tables."""
        try:
            table_names = list(self.analyze_csv_files().keys())
            
            for table_name in table_names:
                sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                self.db_config.execute_query(sql)
                logger.info(f"Dropped table: {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            return False

def main():
    """Main function to generate and create dynamic schemas."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Dynamic Schema Generator')
    parser.add_argument('--data-dir', default='.', help='Data directory containing CSV files')
    parser.add_argument('--drop-first', action='store_true', help='Drop existing tables first')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = DynamicSchemaGenerator(args.data_dir)
    
    try:
        if args.drop_first:
            print("Dropping existing tables...")
            generator.drop_all_tables()
        
        print("Generating dynamic schemas...")
        if generator.create_all_tables():
            print("All tables created successfully!")
        else:
            print("Error creating tables")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        generator.db_config.close_connection()
    
    return True

if __name__ == "__main__":
    main()
