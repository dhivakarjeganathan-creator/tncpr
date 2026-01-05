"""
Response transformation utilities for KPI Timeseries API
Handles timestamp conversion and column name mapping
"""
import json
import logging
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class ResponseTransformer:
    """Transform API responses with timestamp conversion and column mapping"""
    
    def __init__(self, column_mapping_file: str = "original_column_name_schema.json"):
        """
        Initialize transformer with column mapping file
        """
        self.column_mapping = self._load_column_mapping(column_mapping_file)
    
    def _load_column_mapping(self, mapping_file: str) -> Dict[str, Dict[str, str]]:
        """
        Load column mapping from JSON file
        """
        try:
            # Get the directory where this module is located
            module_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Construct full path to the mapping file
            mapping_file_path = os.path.join(module_dir, mapping_file)
            
            logger.info(f"Looking for column mapping file at: {mapping_file_path}")
            
            with open(mapping_file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded column mappings for {len(data)} tables")
            return data
            
        except FileNotFoundError:
            logger.warning(f"Column mapping file not found: {mapping_file}. Column mapping will be skipped.")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse column mapping JSON: {e}. Column mapping will be skipped.")
            return {}
        except Exception as e:
            logger.error(f"Error loading column mapping: {e}. Column mapping will be skipped.")
            return {}
    
    @staticmethod
    def timestamp_to_epoch(timestamp_value: Any) -> int:
        """
        Convert timestamp to epoch milliseconds
        """
        if timestamp_value is None:
            return None
        
        # If already an integer (epoch), return as-is
        if isinstance(timestamp_value, int):
            return timestamp_value
        
        # If it's a float, convert to int
        if isinstance(timestamp_value, float):
            return int(timestamp_value)
        
        # If it's a string, parse it
        if isinstance(timestamp_value, str):
            # Try to parse as integer first (already epoch)
            try:
                return int(timestamp_value)
            except ValueError:
                pass
            
            # Parse as ISO datetime string
            try:                
                # Remove timezone info if present (e.g., " UTC", " EST", " GMT")
                timestamp_str = timestamp_value.strip()
                # Remove timezone abbreviations at the end (3-4 letter codes)
                timestamp_str = re.sub(r'\s+[A-Z]{3,4}$', '', timestamp_str)
                
                # Replace T with space if present
                timestamp_str = timestamp_str.replace('T', ' ')
                
                # Try with milliseconds
                if '.' in timestamp_str:
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                else:
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                # Convert to epoch milliseconds
                epoch_seconds = dt.timestamp()
                epoch_milliseconds = int(epoch_seconds * 1000)
                return epoch_milliseconds
                
            except ValueError as e:
                logger.warning(f"Failed to parse timestamp '{timestamp_value}': {e}. Returning original value.")
                return timestamp_value
        
        # If datetime object
        if isinstance(timestamp_value, datetime):
            epoch_seconds = timestamp_value.timestamp()
            return int(epoch_seconds * 1000)
        
        # Unknown format, return as-is
        logger.warning(f"Unknown timestamp format: {type(timestamp_value)}. Returning original value.")
        return timestamp_value
    
    def map_column_names(self, row: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Map WatsonX column names to original CSV column names
        """
        # Get mapping for this table
        if table_name not in self.column_mapping:
            logger.debug(f"No column mapping found for table '{table_name}'. Using original columns.")
            return row
        
        table_mapping = self.column_mapping[table_name]
        
        # Create new dictionary with mapped column names
        mapped_row = {}
        
        for watsonx_col, value in row.items():
            # Get the original CSV column name from mapping
            # If not in mapping, keep the original name
            csv_col = table_mapping.get(watsonx_col, watsonx_col)
            mapped_row[csv_col] = value
        
        return mapped_row
    
    def transform_results(self,results: List[Dict[str, Any]],table_name: str,convert_timestamp: bool = True,map_columns: bool = True) -> List[Dict[str, Any]]:
        """
        Transform query results by converting timestamps and mapping column names
        """
        if not results:
            return results
        
        transformed_results = []
        
        for row in results:
            # Create a copy to avoid modifying original
            transformed_row = row.copy()
            
            # Step 1: Convert timestamp to epoch milliseconds
            if convert_timestamp and 'timestamp' in transformed_row:
                transformed_row['timestamp'] = self.timestamp_to_epoch(transformed_row['timestamp'])
            
            # Step 2: Map column names from WatsonX to original CSV names
            if map_columns:
                transformed_row = self.map_column_names(transformed_row, table_name)
            
            transformed_results.append(transformed_row)    
        return transformed_results


# Global instance to be used across the application
_transformer_instance = None

def get_transformer(column_mapping_file: str = "original_column_name_schema.json") -> ResponseTransformer:
    """
    Get or create global ResponseTransformer instance
    """
    global _transformer_instance
    
    if _transformer_instance is None:
        _transformer_instance = ResponseTransformer(column_mapping_file)  
    return _transformer_instance