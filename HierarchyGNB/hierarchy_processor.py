#!/usr/bin/env python3
"""
Hierarchy GNB Processor

This script processes the AUPF_ACPF_GNB_Hierarchy.xlsx file and stores the data
in a PostgreSQL database with the specified table structure.

Requirements:
- Parse Excel file with columns: name, asm_unique_id, gnoebId
- Create PostgreSQL table with columns: id, type, typeid, asm_unique_id, gnb
- Transform data according to business rules
"""

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HierarchyProcessor:
    """Processes hierarchy data from Excel to PostgreSQL"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the processor with database configuration
        
        Args:
            db_config: Dictionary containing database connection parameters
        """
        self.db_config = db_config
        self.connection = None
        
    def connect_to_database(self) -> bool:
        """
        Establish connection to PostgreSQL database
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_table(self) -> bool:
        """
        Create the hierarchy table if it doesn't exist
        
        Returns:
            bool: True if table created successfully, False otherwise
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS hierarchy_gnb (
            id SERIAL PRIMARY KEY,
            type VARCHAR(10) NOT NULL CHECK (type IN ('AUPF', 'ACPF')),
            typeid VARCHAR(255) NOT NULL,
            asm_unique_id VARCHAR(255) NOT NULL,
            gnb TEXT NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_hierarchy_type ON hierarchy_gnb(type);
        CREATE INDEX IF NOT EXISTS idx_hierarchy_typeid ON hierarchy_gnb(typeid);
        CREATE INDEX IF NOT EXISTS idx_hierarchy_asm_unique_id ON hierarchy_gnb(asm_unique_id);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("Table 'hierarchy_gnb' created successfully")
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    def read_excel_file(self, file_path: str) -> pd.DataFrame:
        """
        Read the Excel file and return as DataFrame
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            pd.DataFrame: The loaded data
        """
        try:
            df = pd.read_excel(file_path)
            logger.info(f"Successfully loaded Excel file: {file_path}")
            logger.info(f"Data shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to read Excel file: {e}")
            raise
    
    def determine_type(self, name: str) -> str:
        """
        Determine the type based on the name field
        
        Args:
            name: The name field value
            
        Returns:
            str: Either 'AUPF' or 'ACPF'
        """
        if pd.isna(name):
            raise ValueError("Name field cannot be null")
        
        name_str = str(name).upper()
        if 'AUPF' in name_str:
            return 'AUPF'
        elif 'ACPF' in name_str:
            return 'ACPF'
        else:
            raise ValueError(f"Unable to determine type from name: {name}")
    
    def format_gnb(self, gnb_value: str) -> str:
        """
        Format GNB value by adding leading zero if 6 digits
        
        Args:
            gnb_value: The GNB value to format
            
        Returns:
            str: Formatted GNB value
        """
        if pd.isna(gnb_value):
            return ""
        
        gnb_str = str(gnb_value).strip()
        
        # If the value is exactly 6 digits, add leading zero
        if gnb_str.isdigit() and len(gnb_str) == 6:
            return f"0{gnb_str}"
        
        return gnb_str
    
    def split_gnb_values(self, gnb_column: str) -> List[str]:
        """
        Split GNB values by comma and format each value
        
        Args:
            gnb_column: The GNB column value
            
        Returns:
            List[str]: List of formatted GNB values
        """
        if pd.isna(gnb_column):
            return []
        
        gnb_str = str(gnb_column).strip()
        if not gnb_str:
            return []
        
        # Split by comma and clean up each value
        gnb_list = [self.format_gnb(gnb.strip()) for gnb in gnb_str.split(',')]
        
        # Remove empty values
        return [gnb for gnb in gnb_list if gnb]
    
    def transform_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Transform the DataFrame according to business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            List[Dict]: Transformed data ready for database insertion
        """
        transformed_data = []
        
        for index, row in df.iterrows():
            try:
                # Determine type based on name field
                hierarchy_type = self.determine_type(row['name'])
                
                # Get typeid (name field)
                typeid = str(row['name']).strip()
                
                # Get asm_unique_id
                asm_unique_id = str(row['asm_unique_id']).strip() if not pd.isna(row['asm_unique_id']) else ""
                
                # Split and format GNB values
                gnb_values = self.split_gnb_values(row['gnoebId'])
                
                # Create a row for each GNB value
                for gnb in gnb_values:
                    transformed_data.append({
                        'type': hierarchy_type,
                        'typeid': typeid,
                        'asm_unique_id': asm_unique_id,
                        'gnb': gnb
                    })
                
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                logger.error(f"Row data: {row.to_dict()}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} records")
        return transformed_data
    
    def insert_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Insert transformed data into the database
        
        Args:
            data: List of dictionaries containing the data to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        if not data:
            logger.warning("No data to insert")
            return True
        
        insert_sql = """
        INSERT INTO hierarchy_gnb (type, typeid, asm_unique_id, gnb)
        VALUES (%(type)s, %(typeid)s, %(asm_unique_id)s, %(gnb)s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                self.connection.commit()
                logger.info(f"Successfully inserted {len(data)} records")
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to insert data: {e}")
            self.connection.rollback()
            return False
    
    def clear_existing_data(self) -> bool:
        """
        Clear existing data from the table
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM hierarchy_gnb")
                self.connection.commit()
                logger.info("Cleared existing data from hierarchy_gnb table")
                return True
        except psycopg2.Error as e:
            logger.error(f"Failed to clear existing data: {e}")
            return False
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of the inserted data
        
        Returns:
            Dict: Summary statistics
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Total records
                cursor.execute("SELECT COUNT(*) as total FROM hierarchy_gnb")
                total = cursor.fetchone()['total']
                
                # Records by type
                cursor.execute("""
                    SELECT type, COUNT(*) as count 
                    FROM hierarchy_gnb 
                    GROUP BY type
                """)
                type_counts = {row['type']: row['count'] for row in cursor.fetchall()}
                
                # Unique ASM unique IDs
                cursor.execute("SELECT COUNT(DISTINCT asm_unique_id) as unique_asm FROM hierarchy_gnb")
                unique_asm = cursor.fetchone()['unique_asm']
                
                return {
                    'total_records': total,
                    'type_counts': type_counts,
                    'unique_asm_ids': unique_asm
                }
        except psycopg2.Error as e:
            logger.error(f"Failed to get data summary: {e}")
            return {}
    
    def process_file(self, excel_file_path: str, clear_existing: bool = True) -> bool:
        """
        Main method to process the Excel file and store in database
        
        Args:
            excel_file_path: Path to the Excel file
            clear_existing: Whether to clear existing data before inserting
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        try:
            # Connect to database
            if not self.connect_to_database():
                return False
            
            # Create table
            if not self.create_table():
                return False
            
            # Clear existing data if requested
            if clear_existing:
                if not self.clear_existing_data():
                    return False
            
            # Read Excel file
            df = self.read_excel_file(excel_file_path)
            
            # Transform data
            transformed_data = self.transform_data(df)
            
            # Insert data
            if not self.insert_data(transformed_data):
                return False
            
            # Get and display summary
            summary = self.get_data_summary()
            logger.info("Data processing completed successfully!")
            logger.info(f"Summary: {summary}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during file processing: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()
                logger.info("Database connection closed")

def main():
    """Main function to run the hierarchy processor"""
    
    # Import configuration
    from config import DATABASE_CONFIG, EXCEL_FILE_PATH, CLEAR_EXISTING_DATA
    
    # Check if Excel file exists
    if not os.path.exists(EXCEL_FILE_PATH):
        logger.error(f"Excel file not found: {EXCEL_FILE_PATH}")
        sys.exit(1)
    
    # Create processor instance
    processor = HierarchyProcessor(DATABASE_CONFIG)
    
    # Process the file
    success = processor.process_file(EXCEL_FILE_PATH, clear_existing=CLEAR_EXISTING_DATA)
    
    if success:
        logger.info("Processing completed successfully!")
        sys.exit(0)
    else:
        logger.error("Processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
