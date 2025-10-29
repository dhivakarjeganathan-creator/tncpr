"""
Batch Processing Algorithm for Analytics
Based on requirements from batchanalreq2.txt
"""

import logging
import os
import sys
import re
from datetime import datetime, timedelta, time
from typing import List, Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv
import pytz
from logging.handlers import RotatingFileHandler

# Load environment variables from .env file
load_dotenv()

# Configure logging with file output
def setup_logging():
    """Setup logging configuration with both console and file output"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Get log file path from environment or use default
    log_file = os.getenv('LOG_FILE', os.path.join(log_dir, 'batch_processor.log'))
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(getattr(logging, log_level, logging.INFO))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

# Setup logging
logger = setup_logging()

@dataclass
class BatchJobConfig:
    """Configuration for a batch job based on the requirements query"""
    tablename: str
    jobid: int
    event_name: str
    enable_flag: bool
    focal_entity: str
    entity: str
    granularity: str
    job_delay: int
    metricname: str
    aggregation_types: List[str]
    resource_filter: str
    metricid: int
    execution_start_time: datetime

class DatabaseManager:
    """Handles database operations"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Create database connection"""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection established successfully")
        except ImportError:
            logger.error("psycopg2 not installed. Please install it with: pip install psycopg2-binary")
            raise
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a SQL query and return results"""
        try:
            with self.connection.cursor() as cursor:
                logger.info(f"Executing query: {query}")
                if params:
                    logger.info(f"Query parameters: {params}")
                
                cursor.execute(query, params)
                
                # Check if it's a SELECT query (including WITH clauses)
                query_upper = query.strip().upper()
                if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
                    results = cursor.fetchall()
                    # Convert to list of dictionaries
                    return [dict(row) for row in results]
                else:
                    # For INSERT, UPDATE, DELETE queries
                    self.connection.commit()
                    return []
                    
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            self.connection.rollback()
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def get_batch_job_configs(self) -> List[BatchJobConfig]:
        """Get all enabled batch job configurations using the exact query from requirements"""
        query = """
            select d.tablename, a.id jobid, a.event_name, a.enable_flag, b.focal_entity, c.entity, 
            b.granularity, b.job_delay, replace(lower(c.metric_name), '.','_') metricname, c.aggregation_types, b.resource_filter,
            c.id metricid, COALESCE(e.execution_start_time, c.created_at) execution_start_time
            from batch_jobs a inner join batchjob_definitions b on a.id = b.job_id
            inner join batchjob_metrics c on b.id = c.job_id
            inner join metricsandtables d on d.metricname = replace(lower(c.metric_name), '.','_')
            left join batch_job_storage e on c.id = e.jobid
            where a.enable_flag = true  and (e.next_execution_time IS NULL OR e.next_execution_time <= now())
            order by metricid
        """
        
        results = self.execute_query(query)
        logger.info(f"Found {len(results)} enabled batch jobs")
        configs = []
        
        for row in results:
            # Handle aggregation_types as TEXT[] array
            agg_types = row['aggregation_types'] if isinstance(row['aggregation_types'], list) else []
            if isinstance(row['aggregation_types'], str):
                agg_types = row['aggregation_types'].split(',')
            
            config = BatchJobConfig(
                tablename=row['tablename'],
                jobid=row['jobid'],
                event_name=row['event_name'],
                enable_flag=row['enable_flag'],
                focal_entity=row['focal_entity'],
                entity=row['entity'],
                granularity=row['granularity'],
                job_delay=row['job_delay'],
                metricname=row['metricname'],
                aggregation_types=agg_types,
                resource_filter=row['resource_filter'],
                metricid=row['metricid'],
                execution_start_time=row['execution_start_time']
            )
            configs.append(config)
        
        return configs
    
    def create_batch_job_storage_table(self):
        """Create table to store batch job query output and generated SQL queries"""
        query = """
        CREATE TABLE IF NOT EXISTS batch_job_storage (
            id SERIAL PRIMARY KEY,
            jobid INTEGER NOT NULL,
            tablename TEXT NOT NULL,
            metricname TEXT NOT NULL,
            generated_sql_query TEXT NOT NULL,
            execution_start_time TIMESTAMPTZ,
            next_execution_time TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(jobid, tablename, metricname)
        );
        """
        
        self.execute_query(query)
        logger.info("Created batch_job_storage table")

class BatchProcessor:
    """Main batch processing class based on requirements"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def parse_granularity_to_hours(self, granularity: str) -> int:
        """Parse granularity string to hours"""
        if granularity == "1-hour":
            return 1
        elif granularity == "1-day":
            return 24
        elif granularity == "1-week":
            return 168
        else:
            # Handle other formats like "2-hour", "3-day", etc.
            try:
                parts = granularity.split('-')
                if len(parts) == 2:
                    value = int(parts[0])
                    unit = parts[1]
                    if unit == "hour":
                        return value
                    elif unit == "day":
                        return value * 24
                    elif unit == "week":
                        return value * 168
            except ValueError:
                pass
            return 1  # Default to 1 hour
    
    def generate_dynamic_sql_query(self, config: BatchJobConfig) -> tuple[str, datetime, datetime]:
        """Generate dynamic SQL query for each batch job based on requirements"""
        table_name = config.tablename
        metric_name = config.metricname
        granularity_hours = self.parse_granularity_to_hours(config.granularity)
        
        if config.granularity == "1-hour":
            gran = "hour"
            gran_query = "date_trunc('hour', timestamp::timestamp) AS time_bucket"
            execution_start_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        elif config.granularity == "1-day":
            gran = "day"
            gran_query = "CAST(timestamp::timestamp AS DATE) AS time_bucket"
            execution_start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        elif config.granularity == "1-week":
            gran = "week"
            gran_query = "EXTRACT(WEEK FROM  timestamp::timestamp) AS time_bucket"
            execution_start_time = datetime.now() - timedelta(days=datetime.now().weekday()).replace(minute=0, second=0, microsecond=0)
        elif config.granularity == "1-month":
            gran = "month"
            gran_query = "EXTRACT(MONTH FROM  timestamp::timestamp) AS time_bucket"
            execution_start_time = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            gran = "other"
            gran_query = "1 as time_bucket"
            execution_start_time = datetime.now()
        
        # Calculate job delay timestamp
        #execution_start_time = execution_start_time - timedelta(hours=config.job_delay)
        
        # Calculate the end time for the query (current time minus job delay)
        #query_end_time = datetime.now() - timedelta(hours=config.job_delay)
        query_end_time = datetime.now() 
        
        # Step 0: Query to retrieve metricname column, id, timestamp from tablename table
        if table_name == "ruleexecutionresults":
            # For ruleexecutionresults, data is stored as rows
            base_query = f"""
            SELECT 
                "Id" as id, 
                timestamp, 
                udc_config_name as metricname, 
                udc_config_value as metricvalue
            FROM {table_name}
            WHERE replace(lower(udc_config_name), '.','_') = '{metric_name}'
            and created_at > '{config.execution_start_time}' and created_at <= '{query_end_time}'
            """
        else:
            # For other tables, data is stored as columns
            base_query = f"""
            SELECT 
                id, 
                timestamp, 
                {metric_name} as metricvalue
            FROM {table_name}
            Where created_at > '{config.execution_start_time}' and created_at <= '{query_end_time}'
            """
        
        # # Step 2: Add job delay filtering
        # if config.job_delay > 0:
        #     base_query += f" AND timestamp >= '{delay_timestamp}'"
        
        # Add resource filter if provided
        if config.resource_filter:
            base_query += f" AND {config.resource_filter}"

        
        # Step 1 & 3: Generate aggregation query based on granularity and aggregation types
        aggregation_query = f"""
        WITH filtered_data AS (
            {base_query}
        ),
        time_buckets AS (
            SELECT 
                id,
                timestamp,
                metricvalue,
                {gran_query}
            FROM filtered_data
            WHERE metricvalue IS NOT NULL
        )
        SELECT 
            id as id,
            time_bucket as timestamp,
            't' || agg_type || '_' || '{gran}' || '_{metric_name}'  as aggregatedmetricname,
            CASE lower(agg_type)
                WHEN 'sum' THEN SUM(metricvalue::numeric)
                WHEN 'avg' THEN AVG(metricvalue::numeric)
                WHEN 'count' THEN COUNT(metricvalue::numeric)
                WHEN 'min' THEN MIN(metricvalue::numeric)
                WHEN 'max' THEN MAX(metricvalue::numeric)
            END as calculatedaggregatevalue
        FROM time_buckets
        CROSS JOIN unnest(ARRAY{config.aggregation_types}) AS agg_type
        GROUP BY id, time_bucket, agg_type
        ORDER BY id, time_bucket, agg_type
        """

        # Match pattern: "EVERYDAY{hour}{AM/PM}{TZ}"
        match = re.match(r"EVERYDAY(\d{1,2})(AM|PM)([A-Z]+)", config.event_name)

        if match:
            hour = int(match.group(1))
            am_pm = match.group(2)
            tz_name = match.group(3)

            # Convert 12-hour to 24-hour
            if am_pm == "PM" and hour != 12:
                hour += 12
            elif am_pm == "AM" and hour == 12:
                hour = 0

            # Set timezone
            try:
                tz = pytz.timezone(tz_name)
            except Exception:
                tz = pytz.UTC  # fallback

            execution_start_time_tz = execution_start_time.astimezone(tz)

            # Next execution is tomorrow at the target hour
            next_day = execution_start_time_tz.date() + timedelta(days=1)
            next_execution_time = tz.localize(datetime.combine(next_day, time(hour=hour, minute=0, second=0)))

        else:
            if config.event_name == "EVERYHOUR":
                next_execution_time = execution_start_time + timedelta(hours=1)
            elif config.event_name == "EVERYDAY":
                next_execution_time = execution_start_time + timedelta(days=1)
            elif config.event_name == "EVERYWEEK":
                next_execution_time = execution_start_time + timedelta(days=7)
            elif config.event_name == "EVERYMONTH":
                next_execution_time = execution_start_time + timedelta(days=30)
            else:
                next_execution_time = execution_start_time + timedelta(hours=1)
        # Calculate job delay timestamp
        #next_execution_time = next_execution_time - timedelta(hours=config.job_delay)

        return aggregation_query, execution_start_time, next_execution_time
    
    def store_batch_job_query(self, config: BatchJobConfig, generated_sql: str, execution_start_time: datetime = None, next_execution_time: datetime = None):
        """Store batch job query output and generated SQL query in separate table"""
        if execution_start_time is None:
            execution_start_time = datetime.now()
        if next_execution_time is None:
            next_execution_time = datetime.now()
            
        query = """
        INSERT INTO batch_job_storage (jobid, tablename, metricname, generated_sql_query, execution_start_time, next_execution_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (jobid, tablename, metricname) 
        DO UPDATE SET 
            generated_sql_query = EXCLUDED.generated_sql_query,
            execution_start_time = EXCLUDED.execution_start_time,
            next_execution_time = EXCLUDED.next_execution_time,
            created_at = CURRENT_TIMESTAMP
        """
        
        params = (config.metricid, config.tablename, config.metricname, generated_sql, execution_start_time, next_execution_time)
        self.db_manager.execute_query(query, params)
        logger.info(f"Stored/Updated batch job query for metricjob {config.metricid} with execution start time {execution_start_time} and next execution time {next_execution_time}")
    
    def execute_aggregation_query(self, generated_sql: str) -> List[Dict]:
        """Execute the generated aggregation query"""
        return self.db_manager.execute_query(generated_sql)
    
    def store_aggregation_results(self, results: List[Dict]):
        """Store aggregation results in ruleexecutionresults table"""
        for result in results:
            query = """
            INSERT INTO ruleexecutionresults
            ("Id", timestamp, udc_config_name, udc_config_value, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            params = (
                result['id'],
                result['timestamp'],
                result['aggregatedmetricname'],
                result['calculatedaggregatevalue'],
                datetime.now()
            )
            
            self.db_manager.execute_query(query, params)
    
    def process_single_batch_job(self, config: BatchJobConfig):
        """Process a single batch job according to requirements"""
        try:
            logger.info(f"Processing batch job {config.metricid} for table {config.tablename}")
            logger.info(f"Job details - Event: {config.event_name}, Granularity: {config.granularity}, Metric: {config.metricname}")
            
            # Generate dynamic SQL query for this batch job
            logger.debug(f"Generating SQL query for metricjob {config.metricid}")
            generated_sql, execution_start_time, next_execution_time = self.generate_dynamic_sql_query(config)
            logger.debug(f"Generated SQL query: {generated_sql[:200]}...")  # Log first 200 chars
            
            # Store the batch job query output and generated SQL query (without execution start time initially)
            # self.store_batch_job_query(config, generated_sql)
            
            # Execute the aggregation query
            logger.info(f"Executing aggregation query for metricjob {config.metricid}")
            results = self.execute_aggregation_query(generated_sql)
            
            # Update the batch job storage with execution start time and next execution time
            logger.info(f"Storing batch job query for metricjob {config.metricid}")
            self.store_batch_job_query(config, generated_sql, execution_start_time, next_execution_time)
            
            # Store results in RuleExecutionResults table
            if results:
                logger.info(f"Storing {len(results)} aggregation results for metricjob {config.metricid}")
                self.store_aggregation_results(results)
                logger.info(f"Successfully processed {len(results)} aggregation results for metricjob {config.metricid}")
            else:
                logger.warning(f"No results found for metricjob {config.metricid} - no data to process")
                
        except Exception as e:
            logger.error(f"Error processing metricjob {config.metricid}: {e}", exc_info=True)
    
    def process_all_batch_jobs(self):
        """Process all enabled batch jobs"""
        try:
            logger.info("Starting batch job processing")
            
            # Create storage table if it doesn't exist
            logger.info("Ensuring batch_job_storage table exists")
            self.db_manager.create_batch_job_storage_table()
            
            # Get all enabled batch job configurations
            logger.info("Retrieving enabled batch job configurations")
            configs = self.db_manager.get_batch_job_configs()
            logger.info(f"Found {len(configs)} batch job configurations")
            
            # Process each batch job
            processed_count = 0
            for config in configs:
                if config.enable_flag:
                    logger.info(f"Processing batch job {config.metricid} ({processed_count + 1}/{len(configs)})")
                    self.process_single_batch_job(config)
                    processed_count += 1
                else:
                    logger.debug(f"Skipping disabled batch job {config.metricid}")
            
            logger.info(f"Batch processing completed. Processed {processed_count} jobs out of {len(configs)} total configurations")
                    
        except Exception as e:
            logger.error(f"Error processing batch jobs: {e}", exc_info=True)

class BatchProcessingService:
    """Main service class for batch processing"""
    
    def __init__(self, host: str = None, port: int = None, database: str = None, 
                 user: str = None, password: str = None):
        # Load database parameters from environment if not provided
        if host is None:
            # Try to load from .env file if it exists
            env_file = os.path.join(os.path.dirname(__file__), '.env')
            if os.path.exists(env_file):
                load_dotenv(env_file)
            
            # Get individual database parameters
            host = os.getenv('DB_HOST', 'localhost')
            database = os.getenv('DB_NAME')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            port = int(os.getenv('DB_PORT', '5432'))
            
            # Check if all required parameters are provided
            if not all([database, user, password]):
                print("❌ Database connection parameters not found!")
                print("\n📝 To fix this, you have two options:")
                print("\n1️⃣ Create a .env file in your project root with:")
                print("   DB_HOST=localhost")
                print("   DB_NAME=your_database_name")
                print("   DB_USER=your_username")
                print("   DB_PASSWORD=your_password")
                print("   DB_PORT=5432")
                print("\n2️⃣ Or pass the parameters directly:")
                print("   service = BatchProcessingService(host='localhost', database='db', user='user', password='pass')")
                raise ValueError(
                    "Database connection parameters not provided. "
                    "Please create a .env file or pass database parameters directly."
                )
        
        self.db_manager = DatabaseManager(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.processor = BatchProcessor(self.db_manager)
    
    def run_batch_processing(self):
        """Run the batch processing logic"""
        start_time = datetime.now()
        logger.info(f"Starting batch processing at {start_time}")
        
        try:
            self.processor.process_all_batch_jobs()
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Batch processing completed successfully at {end_time}")
            logger.info(f"Total processing time: {duration}")
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error(f"Batch processing failed at {end_time} after {duration}")
            logger.error(f"Error: {e}", exc_info=True)
            raise
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Example usage
if __name__ == "__main__":
    try:
        logger.info("=" * 50)
        logger.info("BATCH PROCESSING SERVICE STARTING")
        logger.info("=" * 50)
        
        # Initialize the service (will read from .env file)
        logger.info("Initializing batch processing service")
        with BatchProcessingService() as service:
            logger.info("Service initialized successfully")
            # Run batch processing
            service.run_batch_processing()
        
        logger.info("=" * 50)
        logger.info("BATCH PROCESSING SERVICE COMPLETED")
        logger.info("=" * 50)
                
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print(f"\n{e}")
        print(f"\n🔧 Quick fix: Run 'python setup_env.py' to create a .env file")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Service error: {e}", exc_info=True)
        sys.exit(1)