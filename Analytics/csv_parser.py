"""
CSV file parser and data validation module.
"""
import pandas as pd
import os
import re
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CSVParser:
    """CSV file parser with validation and data cleaning."""
    
    def __init__(self, data_directory: str = "."):
        """Initialize CSV parser."""
        self.data_directory = data_directory
        self.file_patterns = {
            # Vendor-specific patterns (check these first)
            'CORNING_MKT_LVL': 'MKT_Corning',  # Corning Market files
            'CORNING_GNB_LVL': 'GNB_Corning',  # Corning GNB files
            'CORNING_SECTOR_LVL': 'SECTOR_Corning',  # Corning Sector files
            'CORNING_CARRIER_LVL': 'CARRIER_Corning',  # Corning Carrier files
            'CORNING_DU_LVL': 'DU_Corning',  # Corning Carrier files
            'ER_5G_MKT_LVL': 'MKT_Ericsson',  # Ericsson Market files
            'ER_5G_GNB_LVL': 'GNB_Ericsson',  # Ericsson GNB files
            'ER_5G_SECTOR_LVL': 'SECTOR_Ericsson',  # Ericsson Sector files
            'ER_5G_CARRIER_LVL': 'CARRIER_Ericsson',  # Ericsson Carrier files
            # Generic patterns (check these last)
            '_MKT_LVL': 'MKT_Samsung',  # Samsung Market files
            '_DU_LVL': 'DU_Samsung',    # Samsung DU files
            '_SECTOR_LVL': 'SECTOR_Samsung',  # Samsung Sector files
            '_CARRIER_LVL': 'CARRIER_Samsung',  # Samsung Carrier files            
            'ACPF_GNB_LVL': 'ACPF_GNB_Samsung',  # Samsung GNB files
            'ACPF_VCU_LVL': 'ACPF_VCU_Samsung',  # Samsung VCU files
            '_GNB_LVL': 'AUPF_GNB_Samsung',  # Samsung GNB files
            '_VCU_LVL': 'AUPF_VCU_Samsung',  # Samsung VCU files
            '_VM_ETH_LVL': 'AUPF_VM_Samsung',  # Samsung VCU files
        }
    
    def identify_file_type(self, filename: str) -> Optional[str]:
        """Identify file type based on filename patterns."""
        filename_upper = filename.upper()
        
        for pattern, table_name in self.file_patterns.items():
            if pattern in filename_upper:
            #     # Special handling for VCU files
            #     if pattern == '_VCU_LVL_':
            #         if '13-reports' in filename:
            #             return 'ACPF_Samsung'
            #         elif '12-reports' in filename:
            #             return 'AUPF_Samsung'
            #         else:
            #             return 'ACPF_Samsung'  # Default to ACPF
                return table_name
        
        return None
    
    def get_csv_files(self) -> List[Tuple[str, str]]:
        """Get list of CSV files with their corresponding table names."""
        csv_files = []
        
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                table_name = self.identify_file_type(filename)
                if table_name:
                    csv_files.append((filename, table_name))
                    logger.info(f"Found CSV file: {filename} -> {table_name}")
                else:
                    logger.warning(f"Unknown file type: {filename}")
        
        return csv_files
    
    def clean_column_name(self, col_name: str) -> str:
        """Clean column name for database compatibility."""
        # Remove quotes and special characters
        col_name = col_name.strip("'\"")
        
        # Replace special characters with underscores
        col_name = re.sub(r'[^\w\s]', '_', col_name)
        
        # Replace multiple underscores with single underscore
        col_name = re.sub(r'_+', '_', col_name)
        
        # Remove leading/trailing underscores
        col_name = col_name.strip('_')
        
        # Convert to lowercase
        col_name = col_name.lower()
        
        return col_name
    
    def validate_primary_key(self, row: pd.Series, table_name: str) -> bool:
        """Validate primary key fields are not empty."""
        # All tables now use id and timestamp as primary key
        id_val = row.get('id', '')
        timestamp = row.get('timestamp', '')
        return id_val and id_val != '' and timestamp and timestamp != ''
    
    def clean_data_value(self, value: Any) -> Any:
        """Clean and convert data values."""
        if pd.isna(value) or value == '' or value == 'NaN':
            return None
        
        # Convert string numbers
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.upper() == 'NAN':
                return None
            
            # Handle special values that should remain as strings
            if value in ['Global', '*', 'N/A', 'NULL', 'null', 'None', 'Total *']:
                return value
            
            # Try to convert to number, but preserve leading zeros
            try:
                # Check if value has leading zeros (starts with 0 but is not just "0")
                if value.startswith('0') and len(value) > 1 and value != '0':
                    # Keep as string to preserve leading zeros
                    return value
                
                if '.' in value or 'e' in value.lower():
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                pass
        
        return value
    
    def parse_timestamp_with_timezone(self, timestamp_str: str) -> str:
        """Parse timestamp string and ensure it has timezone information."""
        if not timestamp_str or timestamp_str in ['Total *', 'Global', '*', 'N/A', 'NULL', 'null', 'None']:
            return timestamp_str
        
        try:
            # Try to parse the timestamp
            dt = pd.to_datetime(timestamp_str)
            
            # If it's timezone-naive, assume UTC
            if dt.tz is None:
                dt = dt.tz_localize('UTC')
            
            # Return as ISO format with timezone
            return dt.isoformat()
        except (ValueError, TypeError):
            # If parsing fails, return as-is
            return timestamp_str
    
    def parse_csv_file(self, filepath: str, table_name: str) -> pd.DataFrame:
        """Parse CSV file and return cleaned DataFrame."""
        try:
            logger.info(f"Parsing file: {filepath}")
            
            # Read CSV file
            df = pd.read_csv(filepath)
            
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Clean data values
            for col in df.columns:
                if col.lower() == 'timestamp':
                    # Special handling for timestamp columns to ensure timezone support
                    df[col] = df[col].apply(self.parse_timestamp_with_timezone)
                else:
                    df[col] = df[col].apply(self.clean_data_value)
            
            # Filter out rows with invalid primary keys and "Total *" timestamps
            # don't read the last row. It is a total row.
            valid_rows = []
            for idx, row in df.iloc[:-1].iterrows():
                # Skip rows with "Total *" in timestamp column
                if 'timestamp' in row and row['timestamp'] == 'Total *':
                    logger.warning(f"Skipping row {idx} due to 'Total *' timestamp")
                    continue
                
                if self.validate_primary_key(row, table_name):
                    valid_rows.append(row)
                else:
                    logger.warning(f"Skipping row {idx} due to invalid primary key")
            
            if valid_rows:
                df_clean = pd.DataFrame(valid_rows)
                logger.info(f"Loaded {len(df_clean)} valid rows from {filepath}")
                return df_clean
            else:
                logger.warning(f"No valid rows found in {filepath}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error parsing file {filepath}: {e}")
            return pd.DataFrame()
    
    def get_table_columns(self, df: pd.DataFrame) -> List[str]:
        """Get list of column names from DataFrame."""
        return list(df.columns)
    
    def prepare_data_for_insert(self, df: pd.DataFrame, table_name: str) -> List[Tuple]:
        """Prepare DataFrame data for database insertion."""
        data_tuples = []
        
        for _, row in df.iterrows():
            # Convert row to tuple, handling None values
            row_data = []
            for col in df.columns:
                value = row[col]
                if pd.isna(value) or value is None:
                    row_data.append(None)
                else:
                    row_data.append(value)
            
            data_tuples.append(tuple(row_data))
        
        return data_tuples
    
    def generate_insert_sql(self, table_name: str, columns: List[str]) -> str:
        """Generate INSERT SQL statement."""
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        return f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """
    
    def process_all_files(self) -> Dict[str, pd.DataFrame]:
        """Process all CSV files and return data by table name."""
        csv_files = self.get_csv_files()
        processed_data = {}
        
        for filename, table_name in csv_files:
            filepath = os.path.join(self.data_directory, filename)
            df = self.parse_csv_file(filepath, table_name)
            
            if not df.empty:
                if table_name in processed_data:
                    # Concatenate with existing data
                    processed_data[table_name] = pd.concat([processed_data[table_name], df], ignore_index=True)
                else:
                    processed_data[table_name] = df
        
        return processed_data
