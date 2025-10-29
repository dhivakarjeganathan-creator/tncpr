#!/usr/bin/env python3
"""
Example usage of the Hierarchy GNB Processor

This script demonstrates how to use the HierarchyProcessor class
with custom configuration.
"""

from hierarchy_processor import HierarchyProcessor
from config import DATABASE_CONFIG, EXCEL_FILE_PATH, CLEAR_EXISTING_DATA
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Example usage of the HierarchyProcessor"""
    
    # Create processor instance with configuration
    processor = HierarchyProcessor(DATABASE_CONFIG)
    
    # Process the Excel file
    success = processor.process_file(
        excel_file_path=EXCEL_FILE_PATH,
        clear_existing=CLEAR_EXISTING_DATA
    )
    
    if success:
        logger.info("✅ Processing completed successfully!")
        
        # Get summary of processed data
        summary = processor.get_data_summary()
        logger.info(f"📊 Data Summary:")
        logger.info(f"   Total records: {summary.get('total_records', 0)}")
        logger.info(f"   Records by type: {summary.get('type_counts', {})}")
        logger.info(f"   Unique ASM IDs: {summary.get('unique_asm_ids', 0)}")
    else:
        logger.error("❌ Processing failed!")

if __name__ == "__main__":
    main()
