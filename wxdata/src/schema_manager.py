"""
Schema Manager Module
Handles schema definitions and validation for carrier and du data formats
"""

import json
import os
from typing import Dict, Any, Optional
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import logging

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages schemas for different data formats"""
    
    # Type mapping from JSON schema to PySpark types
    TYPE_MAPPING = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    
    def __init__(self, schemas_dir: str = "schemas"):
        """
        Initialize schema manager
        
        Args:
            schemas_dir: Directory containing schema JSON files
        """
        self.schemas_dir = schemas_dir
        os.makedirs(schemas_dir, exist_ok=True)
        self._initialize_default_schemas()
    
    def _initialize_default_schemas(self):
        """Initialize default schemas if they don't exist"""
        # Default carrier schema
        carrier_schema = {
            "format": "carrier",
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "timestamp", "type": "timestamp", "nullable": False},
                {"name": "carrier_name", "type": "string", "nullable": True},
                {"name": "signal_strength", "type": "double", "nullable": True},
                {"name": "data_usage_mb", "type": "double", "nullable": True},
                {"name": "location", "type": "string", "nullable": True},
            ]
        }
        
        # Default du schema
        du_schema = {
            "format": "du",
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "timestamp", "type": "timestamp", "nullable": False},
                {"name": "device_id", "type": "string", "nullable": True},
                {"name": "du_value", "type": "double", "nullable": True},
                {"name": "status", "type": "string", "nullable": True},
                {"name": "metadata", "type": "string", "nullable": True},
            ]
        }
        
        # Save default schemas if they don't exist
        carrier_path = os.path.join(self.schemas_dir, "carrier_schema.json")
        du_path = os.path.join(self.schemas_dir, "du_schema.json")
        
        if not os.path.exists(carrier_path):
            self.save_schema("carrier", carrier_schema)
            logger.info("Created default carrier schema")
        
        if not os.path.exists(du_path):
            self.save_schema("du", du_schema)
            logger.info("Created default du schema")
    
    def load_schema(self, format_type: str) -> Dict[str, Any]:
        """
        Load schema for a given format type
        
        Args:
            format_type: Format type (carrier or du)
            
        Returns:
            Schema dictionary
        """
        schema_path = os.path.join(self.schemas_dir, f"{format_type}_schema.json")
        
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        logger.info(f"Loaded schema for format: {format_type}")
        return schema
    
    def save_schema(self, format_type: str, schema: Dict[str, Any]):
        """
        Save schema for a given format type
        
        Args:
            format_type: Format type (carrier or du)
            schema: Schema dictionary to save
        """
        schema_path = os.path.join(self.schemas_dir, f"{format_type}_schema.json")
        
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        logger.info(f"Saved schema for format: {format_type}")
    
    def get_spark_schema(self, format_type: str) -> StructType:
        """
        Convert JSON schema to PySpark StructType
        
        Args:
            format_type: Format type (carrier or du)
            
        Returns:
            PySpark StructType
        """
        schema_dict = self.load_schema(format_type)
        fields = []
        
        for field_def in schema_dict.get("fields", []):
            field_name = field_def["name"]
            field_type_str = field_def["type"]
            nullable = field_def.get("nullable", True)
            
            if field_type_str not in self.TYPE_MAPPING:
                logger.warning(f"Unknown type {field_type_str} for field {field_name}, using StringType")
                spark_type = StringType()
            else:
                spark_type = self.TYPE_MAPPING[field_type_str]
            
            fields.append(StructField(field_name, spark_type, nullable=nullable))
        
        return StructType(fields)
    
    def identify_format(self, file_path: str) -> Optional[str]:
        """
        Identify the format type based on file path
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Format type (carrier or du) or None if unknown
        """
        file_path_lower = file_path.lower()
        
        if "carrier" in file_path_lower:
            return "carrier"
        elif "du" in file_path_lower:
            return "du"
        else:
            logger.warning(f"Could not identify format for file: {file_path}")
            return None
    
    def validate_schema(self, format_type: str, schema: Dict[str, Any]) -> bool:
        """
        Validate a schema structure
        
        Args:
            format_type: Format type
            schema: Schema dictionary to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_keys = ["format", "fields"]
        
        if not all(key in schema for key in required_keys):
            logger.error(f"Schema missing required keys: {required_keys}")
            return False
        
        if schema["format"] != format_type:
            logger.error(f"Schema format mismatch: expected {format_type}, got {schema['format']}")
            return False
        
        if not isinstance(schema["fields"], list) or len(schema["fields"]) == 0:
            logger.error("Schema must have at least one field")
            return False
        
        for field in schema["fields"]:
            if not all(key in field for key in ["name", "type"]):
                logger.error(f"Field missing required keys: {field}")
                return False
        
        return True




