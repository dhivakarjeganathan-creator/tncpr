"""
Batch Analytics Data Loader
Loads JSON data from Batch_analytics.json into PostgreSQL database.
Compatible with Python 3.12+
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import sys

from database_config import DatabaseManager, DatabaseConfig, get_db_manager
from env_config import env_config

# Configure logging from environment
log_config = env_config.get_logging_config()
logging.basicConfig(
    level=getattr(logging, log_config['level'].upper()),
    format=log_config['format']
)
logger = logging.getLogger(__name__)

class BatchAnalyticsLoader:
    """Main class for loading batch analytics data into PostgreSQL"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or get_db_manager()
        self.loaded_jobs = 0
        self.skipped_jobs = 0
        self.errors = []
    
    def load_schema(self, schema_file: Optional[str] = None) -> bool:
        """Load database schema from SQL file"""
        try:
            schema_file = schema_file or env_config.SCHEMA_FILE_PATH
            schema_path = Path(schema_file)
            if not schema_path.exists():
                logger.error(f"Schema file {schema_file} not found")
                return False
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute the entire schema as one statement
            # This handles dollar-quoted functions properly
            self.db_manager.execute_update(schema_sql)
            
            logger.info("Database schema loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load database schema: {e}")
            return False
    
    def load_json_data(self, json_file: Optional[str] = None) -> bool:
        """Load data from JSON file into database"""
        try:
            json_file = json_file or env_config.JSON_FILE_PATH
            json_path = Path(json_file)
            if not json_path.exists():
                logger.error(f"JSON file {json_file} not found")
                return False
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                logger.error("JSON file should contain an array of job objects")
                return False
            
            logger.info(f"Loading {len(data)} jobs from {json_file}")
            
            for job_data in data:
                try:
                    self._load_single_job(job_data)
                    self.loaded_jobs += 1
                except Exception as e:
                    logger.error(f"Failed to load job {job_data.get('JOB_NAME', 'Unknown')}: {e}")
                    self.errors.append(f"Job {job_data.get('JOB_NAME', 'Unknown')}: {str(e)}")
                    self.skipped_jobs += 1
            
            logger.info(f"Data loading completed. Loaded: {self.loaded_jobs}, Skipped: {self.skipped_jobs}")
            if self.errors:
                logger.warning(f"Errors encountered: {len(self.errors)}")
                for error in self.errors[:5]:  # Show first 5 errors
                    logger.warning(f"  - {error}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load JSON data: {e}")
            return False
    
    def _load_single_job(self, job_data: Dict[str, Any]) -> None:
        """Load a single job into the database"""
        job_name = job_data.get('JOB_NAME')
        if not job_name:
            raise ValueError("JOB_NAME is required")
        
        # Check if job already exists
        existing_job = self.db_manager.execute_query(
            "SELECT id FROM batch_jobs WHERE job_name = %s",
            (job_name,)
        )
        
        if existing_job:
            logger.warning(f"Job {job_name} already exists, skipping")
            return
        
        # Use a single transaction for the entire job
        with self.db_manager.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Insert job
                    job_id = self._insert_job_with_cursor(cursor, job_data)
                    
                    # Insert job definition
                    self._insert_job_definition_with_cursor(cursor, job_id, job_data.get('DEFINITION', {}))
                    
                    # Insert metrics
                    self._insert_job_metrics_with_cursor(cursor, job_id, job_data.get('DEFINITION', {}).get('metricList', []))
                
                # Commit the transaction
                conn.commit()
                logger.info(f"Job {job_name} loaded successfully with ID {job_id}")
                
            except Exception as e:
                conn.rollback()
                raise e
    
    def _insert_job_with_cursor(self, cursor, job_data: Dict[str, Any]) -> int:
        """Insert job data using cursor and return job ID"""
        query = """
        INSERT INTO batch_jobs (job_name, job_type, event_type, event_name, 
                              update_time, create_time, enable_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        params = (
            job_data.get('JOB_NAME'),
            job_data.get('JOB_TYPE'),
            job_data.get('EVENT_TYPE'),
            job_data.get('EVENT_NAME'),
            job_data.get('UPDATE_TIME'),
            job_data.get('CREATE_TIME'),
            job_data.get('ENABLE_FLAG', True)
        )
        
        logger.info(f"Inserting job: {job_data.get('JOB_NAME')}")
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        if result:
            job_id = result['id']
            logger.info(f"Job inserted successfully with ID: {job_id}")
            return job_id
        else:
            raise ValueError("Failed to get job ID from INSERT")
    
    def _insert_job(self, job_data: Dict[str, Any]) -> int:
        """Insert job data and return job ID"""
        query = """
        INSERT INTO batch_jobs (job_name, job_type, event_type, event_name, 
                              update_time, create_time, enable_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        params = (
            job_data.get('JOB_NAME'),
            job_data.get('JOB_TYPE'),
            job_data.get('EVENT_TYPE'),
            job_data.get('EVENT_NAME'),
            job_data.get('UPDATE_TIME'),
            job_data.get('CREATE_TIME'),
            job_data.get('ENABLE_FLAG', True)
        )
        
        logger.info(f"Inserting job: {job_data.get('JOB_NAME')}")
        logger.debug(f"Job params: {params}")
        
        # Use execute_query for INSERT with RETURNING
        result = self.db_manager.execute_query(query, params)
        if result and len(result) > 0:
            job_id = result[0]['id']
            logger.info(f"Job inserted successfully with ID: {job_id}")
            return job_id
        else:
            raise ValueError("Failed to get job ID from INSERT")
    
    def _insert_job_definition_with_cursor(self, cursor, job_id: int, definition: Dict[str, Any]) -> None:
        """Insert job definition data using cursor"""
        peak_filter = definition.get('peakFilter', {})
        
        query = """
        INSERT INTO batchjob_definitions (job_id, end_time, focal_entity, focal_type, 
                                   granularity, job_delay, job_type, percentile, 
                                   resource_filter, start_time, time_period, timezone,
                                   peak_aggr, peak_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            job_id,
            definition.get('end'),
            definition.get('focalEntity'),
            definition.get('focalType'),
            definition.get('granularity'),
            definition.get('jobDelay', 0),
            definition.get('jobType', 'default'),
            definition.get('percentile', 0),
            definition.get('resourceFilter'),
            definition.get('start'),
            definition.get('time'),
            definition.get('timezone', 'UTC'),
            peak_filter.get('peakAggr'),
            peak_filter.get('points', 0)
        )
        
        cursor.execute(query, params)
        logger.info(f"Job definition inserted for job_id: {job_id}")
    
    def _insert_job_definition(self, job_id: int, definition: Dict[str, Any]) -> None:
        """Insert job definition data"""
        peak_filter = definition.get('peakFilter', {})
        
        query = """
        INSERT INTO batchjob_definitions (job_id, end_time, focal_entity, focal_type, 
                                   granularity, job_delay, job_type, percentile, 
                                   resource_filter, start_time, time_period, timezone,
                                   peak_aggr, peak_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            job_id,
            definition.get('end'),
            definition.get('focalEntity'),
            definition.get('focalType'),
            definition.get('granularity'),
            definition.get('jobDelay', 0),
            definition.get('jobType', 'default'),
            definition.get('percentile', 0),
            definition.get('resourceFilter'),
            definition.get('start'),
            definition.get('time'),
            definition.get('timezone', 'UTC'),
            peak_filter.get('peakAggr'),
            peak_filter.get('points', 0)
        )
        
        self.db_manager.execute_update(query, params)
    
    def _insert_job_metrics_with_cursor(self, cursor, job_id: int, metrics: List[Dict[str, Any]]) -> None:
        """Insert job metrics data using cursor"""
        if not metrics:
            return
        
        query = """
        INSERT INTO batchjob_metrics (job_id, metric_name, entity, aggregation_types)
        VALUES (%s, %s, %s, %s)
        """
        
        for metric in metrics:
            cursor.execute(query, (
                job_id,
                metric.get('name'),
                metric.get('entity'),
                metric.get('aggr', [])
            ))
        
        logger.info(f"Inserted {len(metrics)} metrics for job_id: {job_id}")
    
    def _insert_job_metrics(self, job_id: int, metrics: List[Dict[str, Any]]) -> None:
        """Insert job metrics data"""
        if not metrics:
            return
        
        query = """
        INSERT INTO batchjob_metrics (job_id, metric_name, entity, aggregation_types)
        VALUES (%s, %s, %s, %s)
        """
        
        params_list = []
        for metric in metrics:
            params_list.append((
                job_id,
                metric.get('name'),
                metric.get('entity'),
                metric.get('aggr', [])
            ))
        
        self.db_manager.execute_batch(query, params_list)
    
    def get_loading_summary(self) -> Dict[str, Any]:
        """Get summary of loading operation"""
        return {
            'loaded_jobs': self.loaded_jobs,
            'skipped_jobs': self.skipped_jobs,
            'total_errors': len(self.errors),
            'errors': self.errors
        }

def main():
    """Main function to run the data loader"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load batch analytics data into PostgreSQL')
    parser.add_argument('--json-file', default=env_config.JSON_FILE_PATH, 
                       help=f'Path to JSON file (default: {env_config.JSON_FILE_PATH})')
    parser.add_argument('--schema-file', default=env_config.SCHEMA_FILE_PATH,
                       help=f'Path to schema file (default: {env_config.SCHEMA_FILE_PATH})')
    parser.add_argument('--init-schema', action='store_true',
                       help='Initialize database schema before loading data')
    # Database configuration is now handled by .env file
    # Command-line overrides are not supported to maintain single source of truth
    
    args = parser.parse_args()
    
    try:
        # Initialize database manager (will use env_config automatically)
        db_manager = DatabaseManager()
        
        # Test connection
        if not db_manager.test_connection():
            logger.error("Failed to connect to database")
            sys.exit(1)
        
        logger.info("Database connection successful")
        
        # Initialize loader
        loader = BatchAnalyticsLoader(db_manager)
        
        # Load schema if requested
        if args.init_schema:
            if not loader.load_schema(args.schema_file):
                logger.error("Failed to load database schema")
                sys.exit(1)
        
        # Load JSON data
        if not loader.load_json_data(args.json_file):
            logger.error("Failed to load JSON data")
            sys.exit(1)
        
        # Print summary
        summary = loader.get_loading_summary()
        print("\n" + "="*50)
        print("LOADING SUMMARY")
        print("="*50)
        print(f"Jobs loaded successfully: {summary['loaded_jobs']}")
        print(f"Jobs skipped: {summary['skipped_jobs']}")
        print(f"Total errors: {summary['total_errors']}")
        
        if summary['errors']:
            print("\nFirst 5 errors:")
            for error in summary['errors'][:5]:
                print(f"  - {error}")
        
        print("="*50)
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if 'db_manager' in locals():
            db_manager.close_pool()

if __name__ == "__main__":
    main()
