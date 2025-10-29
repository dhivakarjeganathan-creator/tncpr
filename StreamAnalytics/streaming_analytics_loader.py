"""
Streaming Analytics Data Loader for PostgreSQL
This script loads streaming analytics data from JSON and stores it in PostgreSQL database.
"""

import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import os
from config import Config

# Python 3.12+ compatibility
try:
    from python312_compat import check_compatibility
    check_compatibility()
except ImportError:
    pass

# Configure logging
log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StreamingAnalyticsLoader:
    """Class to handle loading streaming analytics data into PostgreSQL database."""
    
    def __init__(self, db_config: Dict[str, str] = None):
        """
        Initialize the loader with database configuration.
        
        Args:
            db_config: Dictionary containing database connection parameters.
                      If None, will use configuration from config.py
        """
        self.db_config = db_config or Config.get_db_config()
        self.connection = None
        
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port']
            )
            self.connection.autocommit = False
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_tables(self):
        """Create database tables if they don't exist."""
        try:
            with open('database_schema.sql', 'r') as f:
                schema_sql = f.read()
            
            with self.connection.cursor() as cursor:
                cursor.execute(schema_sql)
                self.connection.commit()
                logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            self.connection.rollback()
            raise
    
    def load_json_data(self, json_file_path: str) -> List[Dict[str, Any]]:
        """
        Load streaming analytics data from JSON file.
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            List of streaming analytics records
        """
        try:
            with open(json_file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} records from {json_file_path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load JSON data: {e}")
            raise
    
    def insert_job(self, job_data: Dict[str, Any]) -> int:
        """
        Insert a single job record into the database.
        
        Args:
            job_data: Dictionary containing job information
            
        Returns:
            Job ID of the inserted record
        """
        try:
            with self.connection.cursor() as cursor:
                # Insert into streaming_jobs table
                cursor.execute("""
                    INSERT INTO streaming_jobs (job_name, job_type, event_type, event_name, 
                                             update_time, create_time, enable_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_name) DO UPDATE SET
                        job_type = EXCLUDED.job_type,
                        event_type = EXCLUDED.event_type,
                        event_name = EXCLUDED.event_name,
                        update_time = EXCLUDED.update_time,
                        create_time = EXCLUDED.create_time,
                        enable_flag = EXCLUDED.enable_flag,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    job_data['JOB_NAME'],
                    job_data['JOB_TYPE'],
                    job_data['EVENT_TYPE'],
                    job_data.get('EVENT_NAME', ''),
                    job_data['UPDATE_TIME'],
                    job_data['CREATE_TIME'],
                    job_data['ENABLE_FLAG']
                ))
                
                job_id = cursor.fetchone()[0]
                
                # Insert into job_definitions table
                definition = job_data['DEFINITION']
                
                # First, try to delete existing definition if it exists
                cursor.execute("DELETE FROM job_definitions WHERE job_id = %s", (job_id,))
                
                # Then insert the new definition
                cursor.execute("""
                    INSERT INTO job_definitions (job_id, focal_entity, focal_type, 
                                               resource_filter, stream_name, window_gran)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    job_id,
                    definition['focalEntity'],
                    definition['focalType'],
                    definition.get('resourceFilter', ''),
                    definition['streamName'],
                    definition['windowGran']
                ))
                
                definition_id = cursor.fetchone()[0]
                
                # Clear existing metrics for this job definition
                cursor.execute("DELETE FROM job_metrics WHERE job_definition_id = %s", (definition_id,))
                
                # Insert metrics
                for metric in definition['metricList']:
                    for aggregation in metric['aggr']:
                        cursor.execute("""
                            INSERT INTO job_metrics (job_definition_id, entity, metric_name, aggregation_type)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            definition_id,
                            metric['entity'],
                            metric['name'],
                            aggregation
                        ))
                
                self.connection.commit()
                logger.info(f"Successfully inserted job: {job_data['JOB_NAME']}")
                return job_id
                
        except Exception as e:
            logger.error(f"Failed to insert job {job_data.get('JOB_NAME', 'Unknown')}: {e}")
            self.connection.rollback()
            raise
    
    def load_all_data(self, json_file_path: str):
        """
        Load all streaming analytics data from JSON file into database.
        
        Args:
            json_file_path: Path to the JSON file
        """
        try:
            data = self.load_json_data(json_file_path)
            
            for job_data in data:
                self.insert_job(job_data)
            
            logger.info(f"Successfully loaded {len(data)} jobs into database")
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def get_job_by_name(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job by its name.
        
        Args:
            job_name: Name of the job to retrieve
            
        Returns:
            Dictionary containing job information or None if not found
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM job_complete_info WHERE job_name = %s
                """, (job_name,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            logger.error(f"Failed to retrieve job {job_name}: {e}")
            return None
    
    def get_jobs_by_entity(self, entity: str) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs for a specific entity.
        
        Args:
            entity: Entity name to filter by
            
        Returns:
            List of job dictionaries
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM job_complete_info WHERE focal_entity = %s
                """, (entity,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to retrieve jobs for entity {entity}: {e}")
            return []
    
    def get_jobs_by_window_granularity(self, window_gran: str) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a specific window granularity.
        
        Args:
            window_gran: Window granularity to filter by (e.g., '1-minute', '5-minute')
            
        Returns:
            List of job dictionaries
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM job_complete_info WHERE window_gran = %s
                """, (window_gran,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to retrieve jobs for window granularity {window_gran}: {e}")
            return []
    
    def get_metrics_for_job(self, job_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all metrics for a specific job.
        
        Args:
            job_name: Name of the job
            
        Returns:
            List of metric dictionaries
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM job_metrics_complete WHERE job_name = %s
                """, (job_name,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to retrieve metrics for job {job_name}: {e}")
            return []
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs from the database.
        
        Returns:
            List of all job dictionaries
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM job_complete_info ORDER BY job_name")
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to retrieve all jobs: {e}")
            return []
    
    def get_jobs_by_aggregation_type(self, aggregation_type: str) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs that use a specific aggregation type.
        
        Args:
            aggregation_type: Type of aggregation (e.g., 'sum', 'Average', 'Max')
            
        Returns:
            List of job dictionaries
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT jci.* FROM job_complete_info jci
                    JOIN job_metrics jm ON jci.definition_id = jm.job_definition_id
                    WHERE jm.aggregation_type = %s
                    ORDER BY jci.job_name
                """, (aggregation_type,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to retrieve jobs for aggregation type {aggregation_type}: {e}")
            return []


def main():
    """Main function to demonstrate usage."""
    # Validate configuration
    if not Config.validate_config():
        print("Configuration validation failed. Please check your .env file or environment variables.")
        return
    
    # Initialize loader (will use config from config.py)
    loader = StreamingAnalyticsLoader()
    
    try:
        # Connect to database
        loader.connect()
        
        # Create tables
        loader.create_tables()
        
        # Load data from JSON file
        loader.load_all_data('Streaming_analytics.json')
        
        # Example queries
        print("\n=== Example Queries ===")
        
        # Get all jobs
        all_jobs = loader.get_all_jobs()
        print(f"Total jobs loaded: {len(all_jobs)}")
        
        # Get jobs by entity
        gnb_jobs = loader.get_jobs_by_entity('GNB')
        print(f"GNB jobs: {len(gnb_jobs)}")
        
        # Get jobs by window granularity
        minute_jobs = loader.get_jobs_by_window_granularity('1-minute')
        print(f"1-minute window jobs: {len(minute_jobs)}")
        
        # Get metrics for a specific job
        if all_jobs:
            first_job = all_jobs[0]
            metrics = loader.get_metrics_for_job(first_job['job_name'])
            print(f"Metrics for {first_job['job_name']}: {len(metrics)}")
        
        # Get jobs by aggregation type
        sum_jobs = loader.get_jobs_by_aggregation_type('sum')
        print(f"Jobs using 'sum' aggregation: {len(sum_jobs)}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        loader.disconnect()


if __name__ == "__main__":
    main()
