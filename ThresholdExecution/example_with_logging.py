"""
Example script demonstrating how to use the Threshold Execution System with file logging.
The threshold_processor automatically creates timestamped log files.
"""

import logging
from threshold_processor import ThresholdProcessor
from database import DatabaseConnection

def main():
    """Main function demonstrating logging setup and usage."""
    
    # Basic logging setup - threshold_processor will create its own log file
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Starting Threshold Execution System example")
    
    try:
        # Initialize database connection (replace with your actual connection)
        # db_connection = DatabaseConnection(...)
        
        # For this example, we'll use a mock connection
        from test_system import MockDatabaseConnection
        db_connection = MockDatabaseConnection()
        
        # Create threshold processor
        processor = ThresholdProcessor(db_connection)
        logger.info("Threshold processor initialized successfully")
        
        # Process thresholds (this will generate logs)
        logger.info("Starting threshold processing...")
        results = processor.process_all_thresholds()
        
        logger.info(f"Threshold processing completed. Processed {len(results)} threshold jobs")
        
        # Log some example results
        for result in results[:3]:  # Log first 3 results
            logger.info(f"Threshold ID: {result.get('threshold_id')}, "
                       f"Record Count: {result.get('record_count')}")
        
        logger.info("Example completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)
        raise
    
    finally:
        logger.info(f"Logs have been saved to: {log_file}")

if __name__ == "__main__":
    main()
