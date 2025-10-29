"""
Main execution script for threshold query generation system.
"""

import logging
import os
from typing import List, Dict, Any
from database import DatabaseConnection
from threshold_processor import ThresholdProcessor
from schema import get_create_threshold_queries_table_ddl, get_create_indexes_ddl, get_add_record_count_column_ddl, get_add_execution_datetime_column_ddl, get_create_alarm_table_ddl, get_create_alarm_indexes_ddl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_database_schema(db: DatabaseConnection) -> None:
    """Set up the database schema for storing generated queries."""
    try:
        logger.info("Setting up database schema...")
        
        # Create the main table
        create_table_ddl = get_create_threshold_queries_table_ddl()
        db.execute_ddl(create_table_ddl)
        
        # Add record_count column to existing table (if it doesn't exist)
        add_column_ddl = get_add_record_count_column_ddl()
        db.execute_ddl(add_column_ddl)
        
        # Add execution_datetime column to existing table (if it doesn't exist)
        add_execution_datetime_ddl = get_add_execution_datetime_column_ddl()
        db.execute_ddl(add_execution_datetime_ddl)
        
        # Create alarm table
        create_alarm_table_ddl = get_create_alarm_table_ddl()
        db.execute_ddl(create_alarm_table_ddl)
        
        # Create indexes
        create_indexes_ddl = get_create_indexes_ddl()
        db.execute_ddl(create_indexes_ddl)
        
        # Create alarm table indexes
        create_alarm_indexes_ddl = get_create_alarm_indexes_ddl()
        db.execute_ddl(create_alarm_indexes_ddl)
        
        logger.info("Database schema setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database schema: {e}")
        raise

def display_generated_queries(results: List[Dict[str, Any]]) -> None:
    """Display the generated queries and execution results for each threshold job."""
    logger.info(f"\n{'='*80}")
    logger.info("GENERATED QUERIES AND EXECUTION RESULTS FOR EACH THRESHOLD JOB")
    logger.info(f"{'='*80}")
    
    total_records = 0
    
    for i, result in enumerate(results, 1):
        logger.info(f"\n--- THRESHOLD JOB #{i} ---")
        logger.info(f"Threshold ID: {result.get('threshold_id')}")
        logger.info(f"Table Name: {result.get('tablename')}")
        logger.info(f"Metric Name: {result.get('metricname')}")
        logger.info(f"Lower Limit: {result.get('lowerlimit')}")
        logger.info(f"Upper Limit: {result.get('upperlimit')}")
        logger.info(f"Occurrence: {result.get('occurrence')}")
        logger.info(f"Time (hours): {result.get('time')}")
        logger.info(f"Mode: {result.get('mode')}")
        logger.info(f"Category: {result.get('category')}")
        logger.info(f"Resource: {result.get('resource')}")
        logger.info(f"Threshold Group: {result.get('threshold_group')}")
        
        # Display execution results
        record_count = result.get('record_count', 0)
        execution_datetime = result.get('execution_datetime')
        total_records += record_count
        logger.info(f"\n📊 EXECUTION RESULTS:")
        logger.info(f"Records Returned: {record_count}")
        logger.info(f"Execution DateTime: {execution_datetime}")
        
        logger.info(f"\nGenerated SQL Query:")
        logger.info(f"{'-'*60}")
        logger.info(result.get('generated_sql_query', ''))
        logger.info(f"{'-'*60}")
    
    logger.info(f"\n{'='*80}")
    logger.info(f"SUMMARY: Total records returned across all queries: {total_records}")
    logger.info(f"{'='*80}")

def save_generated_queries(db: DatabaseConnection, results: List[Dict[str, Any]]) -> None:
    """Save the generated queries to the database."""
    try:
        if not results:
            logger.warning("No results to save")
            return
        
        # Get the table name from environment
        table_name = os.getenv('GENERATED_QUERIES_TABLE', 'threshold_generated_queries')
        
        # Clear existing data for this run (optional)
        clear_query = f"DELETE FROM {table_name}"
        db.execute_ddl(clear_query)
        logger.info("Cleared existing generated queries")
        
        # Insert new data
        db.insert_data(table_name, results)
        logger.info(f"Successfully saved {len(results)} generated queries to {table_name}")
        
    except Exception as e:
        logger.error(f"Error saving generated queries: {e}")
        raise

def main():
    """Main execution function."""
    try:
        logger.info("Starting Threshold Query Generation System")
        logger.info("="*50)
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = DatabaseConnection()
        
        # Set up database schema
        setup_database_schema(db)
        
        # Initialize threshold processor
        logger.info("Initializing threshold processor...")
        processor = ThresholdProcessor(db)
        
        # Process all thresholds, generate queries, and execute them
        logger.info("Processing threshold jobs, generating queries, and executing them...")
        results = processor.process_all_thresholds()
        
        if not results:
            logger.warning("No threshold jobs found or processed")
            return
        
        # Display generated queries and execution results
        display_generated_queries(results)
        
        # Save generated queries and execution results to database
        logger.info("Saving generated queries and execution results to database...")
        save_generated_queries(db, results)
        
        logger.info("="*50)
        logger.info("Threshold Query Generation and Execution System completed successfully!")
        logger.info(f"Total threshold jobs processed: {len(results)}")
        
        # Calculate and display total records
        total_records = sum(result.get('record_count', 0) for result in results)
        logger.info(f"Total records returned across all queries: {total_records}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
