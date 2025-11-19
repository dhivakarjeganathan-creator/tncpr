"""
Iceberg Manager Module
Handles Iceberg table operations using Spark
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
import logging
import threading

logger = logging.getLogger(__name__)


class IcebergManager:
    """Manages Iceberg table operations"""
    
    def __init__(self, spark: SparkSession, catalog_name: str, database_name: str):
        """
        Initialize Iceberg manager
        
        Args:
            spark: SparkSession instance
            catalog_name: Name of the catalog
            database_name: Name of the database
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        self._table_creation_locks = {}  # Locks for thread-safe table creation
        self._table_creation_lock = threading.Lock()  # Lock for managing table creation locks
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Create database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")
            logger.info(f"Database {self.database_name} ensured to exist")
        except Exception as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            result = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{self.database_name}"
            ).filter(col("tableName") == table_name).collect()
            return len(result) > 0
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False
    
    def create_table(self, table_name: str, schema, partition_columns: List[str] = None,
                     table_properties: Dict[str, str] = None):
        """
        Create an Iceberg table (thread-safe)
        
        Args:
            table_name: Name of the table
            schema: PySpark StructType schema
            partition_columns: List of partition column names
            table_properties: Optional table properties
        """
        if partition_columns is None:
            partition_columns = []
        
        if table_properties is None:
            table_properties = {}
        
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        # Get or create a lock for this specific table to prevent race conditions
        with self._table_creation_lock:
            if table_name not in self._table_creation_locks:
                self._table_creation_locks[table_name] = threading.Lock()
            table_lock = self._table_creation_locks[table_name]
        
        # Use table-specific lock to ensure only one thread creates the table
        with table_lock:
            # Double-check if table exists after acquiring lock
            if self.table_exists(table_name):
                logger.debug(f"Table {full_table_name} already exists, skipping creation")
                return
            
            try:
                # Create empty DataFrame with schema
                empty_df = self.spark.createDataFrame([], schema)
                
                # Write as Iceberg table
                writer = empty_df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", f"{self.catalog_name}.{self.database_name}.{table_name}")
                
                if partition_columns:
                    writer = writer.partitionBy(*partition_columns)
                
                # Apply table properties if provided
                if table_properties:
                    for key, value in table_properties.items():
                        writer = writer.option(f"write.metadata.{key}", value)
                
                writer.saveAsTable(full_table_name)
                
                logger.info(f"Created Iceberg table: {full_table_name}")
            except Exception as e:
                # If table was created by another thread, that's okay
                if self.table_exists(table_name):
                    logger.debug(f"Table {full_table_name} was created by another thread")
                else:
                    logger.error(f"Error creating table {full_table_name}: {e}")
                    raise
    
    def append_data(self, table_name: str, df: DataFrame):
        """
        Append data to an Iceberg table
        
        Args:
            table_name: Name of the table
            df: DataFrame to append
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        try:
            df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(full_table_name)
            
            logger.info(f"Appended data to table: {full_table_name}")
        except Exception as e:
            logger.error(f"Error appending data to table {full_table_name}: {e}")
            raise
    
    def load_csv_to_table(self, csv_path: str, table_name: str, schema, 
                         format_type: str = None):
        """
        Load CSV file into Iceberg table
        
        Args:
            csv_path: Path to CSV file (S3 path)
            table_name: Name of the target table
            schema: PySpark StructType schema
            format_type: Optional format type for logging
        """
        try:
            # Read CSV with schema
            df = self.spark.read \
                .schema(schema) \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .csv(csv_path)
            
            # Add ingestion timestamp
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            
            # Append to table
            self.append_data(table_name, df)
            
            logger.info(f"Loaded CSV {csv_path} to table {table_name} (format: {format_type})")
        except Exception as e:
            logger.error(f"Error loading CSV {csv_path} to table {table_name}: {e}")
            raise
    
    def load_csv_files_to_table(self, csv_paths: List[str], table_name: str, schema, 
                                format_type: str = None):
        """
        Load multiple CSV files into Iceberg table in a single Spark job
        
        This is more efficient than loading files one by one as it uses a single
        Spark read operation for all files.
        
        Args:
            csv_paths: List of paths to CSV files (S3 paths)
            table_name: Name of the target table
            schema: PySpark StructType schema
            format_type: Optional format type for logging
        """
        if not csv_paths:
            logger.warning(f"No CSV paths provided for table {table_name}")
            return
        
        try:
            # Read all CSV files in a single Spark job
            # PySpark's csv() method accepts multiple paths when unpacked with *
            # This reads all files in parallel and combines them into one DataFrame
            if len(csv_paths) == 1:
                df = self.spark.read \
                    .schema(schema) \
                    .option("header", "true") \
                    .option("inferSchema", "false") \
                    .csv(csv_paths[0])
            else:
                # Unpack list to pass multiple paths - Spark reads all files in one job
                df = self.spark.read \
                    .schema(schema) \
                    .option("header", "true") \
                    .option("inferSchema", "false") \
                    .csv(*csv_paths)  # Unpack list - Spark reads all files in parallel
            
            # Add ingestion timestamp
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            
            # Append to table in a single write operation
            self.append_data(table_name, df)
            
            logger.info(f"Loaded {len(csv_paths)} CSV files to table {table_name} (format: {format_type})")
        except Exception as e:
            logger.error(f"Error loading {len(csv_paths)} CSV files to table {table_name}: {e}")
            raise
    
    def compact_table(self, table_name: str):
        """
        Compact Iceberg table metadata (rewrite small files)
        
        Args:
            table_name: Name of the table
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        try:
            # Use Spark SQL to rewrite data files
            self.spark.sql(f"CALL {self.catalog_name}.system.rewrite_data_files('{full_table_name}')")
            logger.info(f"Compacted table: {full_table_name}")
        except Exception as e:
            logger.warning(f"Error compacting table {full_table_name}: {e}")
            # Compaction is optional, so we log as warning
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        try:
            # Get table schema
            df = self.spark.table(full_table_name)
            schema = df.schema
            
            # Get row count
            row_count = df.count()
            
            return {
                "table_name": full_table_name,
                "schema": schema.json(),
                "row_count": row_count
            }
        except Exception as e:
            logger.error(f"Error getting table info for {full_table_name}: {e}")
            return {}
    
    def query_table(self, table_name: str, query: str = None) -> DataFrame:
        """
        Query an Iceberg table
        
        Args:
            table_name: Name of the table
            query: Optional SQL query (if None, returns all data)
            
        Returns:
            DataFrame with query results
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        try:
            if query:
                # Execute custom query
                sql_query = query.replace("{table}", full_table_name)
                return self.spark.sql(sql_query)
            else:
                # Return all data
                return self.spark.table(full_table_name)
        except Exception as e:
            logger.error(f"Error querying table {full_table_name}: {e}")
            raise

