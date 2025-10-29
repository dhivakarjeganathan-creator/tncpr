"""
Example usage of the Threshold Execution System.
This script demonstrates how to use the system programmatically.
"""

import logging
from database import DatabaseConnection
from threshold_processor import ThresholdProcessor
from schema import get_create_threshold_queries_table_ddl, get_create_indexes_ddl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def example_usage():
    """Example of how to use the threshold execution system."""
    
    try:
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = DatabaseConnection()
        
        # Set up database schema
        logger.info("Setting up database schema...")
        db.execute_ddl(get_create_threshold_queries_table_ddl())
        db.execute_ddl(get_create_indexes_ddl())
        
        # Initialize threshold processor
        logger.info("Initializing threshold processor...")
        processor = ThresholdProcessor(db)
        
        # Get threshold jobs
        logger.info("Retrieving threshold jobs...")
        threshold_jobs = processor.get_threshold_jobs()
        logger.info(f"Found {len(threshold_jobs)} threshold jobs")
        
        # Process a single threshold job (example)
        if threshold_jobs:
            first_job = threshold_jobs[0]
            logger.info(f"Processing first threshold job: {first_job.get('threshold_id')}")
            
            generated_query = processor.generate_metric_query(first_job)
            logger.info("Generated query:")
            logger.info("-" * 50)
            logger.info(generated_query)
            logger.info("-" * 50)
        
        # Process all threshold jobs
        logger.info("Processing all threshold jobs...")
        results = processor.process_all_thresholds()
        logger.info(f"Successfully processed {len(results)} threshold jobs")
        
        # Display first few results
        for i, result in enumerate(results[:3]):  # Show first 3 results
            logger.info(f"\nResult {i+1}:")
            logger.info(f"  Threshold ID: {result.get('threshold_id')}")
            logger.info(f"  Table: {result.get('tablename')}")
            logger.info(f"  Metric: {result.get('metricname')}")
            logger.info(f"  Query length: {len(result.get('generated_sql_query', ''))} characters")
        
        logger.info("Example usage completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in example usage: {e}")
        raise

if __name__ == "__main__":
    example_usage()
