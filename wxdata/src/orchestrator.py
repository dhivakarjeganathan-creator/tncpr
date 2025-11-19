"""
Orchestrator Module
Main automation logic that coordinates all components
"""

import time
import yaml
from typing import Dict, Any
from pyspark.sql import SparkSession
import logging

from .minio_client import MinIOClient
from .schema_manager import SchemaManager
from .iceberg_manager import IcebergManager
from .file_processor import FileProcessor

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the entire CSV to Iceberg pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize orchestrator with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        
        # Initialize components
        self.spark = self._create_spark_session()
        self.minio_client = self._create_minio_client()
        self.schema_manager = SchemaManager()
        self.iceberg_manager = self._create_iceberg_manager()
        self.file_processor = FileProcessor(
            self.minio_client,
            self.schema_manager,
            self.iceberg_manager
        )
        
        logger.info("Pipeline orchestrator initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO"))
        log_file = log_config.get("file", "logs/pipeline.log")
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # Create logs directory
        import os
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg configuration"""
        spark_config = self.config.get("spark", {})
        app_name = spark_config.get("app_name", "CSV to Iceberg Pipeline")
        master = spark_config.get("master", "local[*]")
        config_dict = spark_config.get("config", {})
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)
        
        # Add all Spark configurations
        for key, value in config_dict.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info("Spark session created")
        return spark
    
    def _create_minio_client(self) -> MinIOClient:
        """Create MinIO client"""
        minio_config = self.config.get("minio", {})
        client = MinIOClient(
            endpoint=minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config.get("secure", False),
            bucket_name=minio_config["bucket_name"]
        )
        logger.info("MinIO client created")
        return client
    
    def _create_iceberg_manager(self) -> IcebergManager:
        """Create Iceberg manager"""
        watsonx_config = self.config.get("watsonx", {})
        manager = IcebergManager(
            self.spark,
            catalog_name=watsonx_config["catalog_name"],
            database_name=watsonx_config["database_name"]
        )
        logger.info("Iceberg manager created")
        return manager
    
    def run_single_cycle(self) -> Dict[str, int]:
        """
        Run a single processing cycle
        
        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting processing cycle")
        
        minio_config = self.config.get("minio", {})
        landing_prefix = minio_config.get("landing_prefix", "landing/Samsung")
        processed_prefix = minio_config.get("processed_prefix", "processed/Samsung")
        watsonx_config = self.config.get("watsonx", {})
        warehouse_path = watsonx_config.get("warehouse_path", "s3://samsung-data/warehouse")
        table_configs = self.config.get("tables", {})
        processing_config = self.config.get("processing", {})
        batch_size = processing_config.get("batch_size", 100)
        max_workers = processing_config.get("max_workers", 10)
        
        # Detect new files
        new_files = self.file_processor.detect_new_files(landing_prefix)
        
        if not new_files:
            logger.info("No new files to process")
            return {"processed": 0, "failed": 0, "total": 0}
        
        # Process in batches
        total_processed = 0
        total_failed = 0
        
        # Use grouped processing by default (loads all files for each table in single Spark job)
        # Set use_grouped_processing to False in config to use parallel file-by-file processing
        use_grouped_processing = processing_config.get("use_grouped_processing", True)
        
        if use_grouped_processing:
            # Process all files grouped by table - more efficient for Spark
            # Each table group will be processed in parallel using threads
            logger.info(f"Processing {len(new_files)} files grouped by table (max_workers: {max_workers})")
            successful, failed = self.file_processor.process_batch_grouped_by_table(
                new_files,
                table_configs,
                landing_prefix,
                processed_prefix,
                warehouse_path,
                max_workers=max_workers
            )
            total_processed = successful
            total_failed = failed
        else:
            # Process in batches with parallel file-by-file processing
            for i in range(0, len(new_files), batch_size):
                batch = new_files[i:i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} files)")
                
                successful, failed = self.file_processor.process_batch(
                    batch,
                    table_configs,
                    landing_prefix,
                    processed_prefix,
                    warehouse_path,
                    max_workers=max_workers
                )
                
                total_processed += successful
                total_failed += failed
        
        # Optional compaction
        if processing_config.get("enable_compaction", False):
            self._run_compaction(table_configs, processing_config.get("compaction_threshold", 10))
        
        result = {
            "processed": total_processed,
            "failed": total_failed,
            "total": len(new_files)
        }
        
        logger.info(f"Processing cycle complete: {result}")
        return result
    
    def _run_compaction(self, table_configs: Dict, threshold: int):
        """Run compaction on tables if needed"""
        for format_type, table_config in table_configs.items():
            table_name = table_config["table_name"]
            if self.file_processor.should_compact(table_name, threshold):
                logger.info(f"Running compaction on table: {table_name}")
                try:
                    self.iceberg_manager.compact_table(table_name)
                except Exception as e:
                    logger.warning(f"Compaction failed for {table_name}: {e}")
    
    def run_continuous(self):
        """Run the pipeline continuously, checking for new files at intervals"""
        processing_config = self.config.get("processing", {})
        check_interval = processing_config.get("check_interval_seconds", 60)
        
        logger.info(f"Starting continuous processing (check interval: {check_interval}s)")
        
        try:
            while True:
                self.run_single_cycle()
                time.sleep(check_interval)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        finally:
            self.cleanup()
    
    def run_once(self):
        """Run the pipeline once and exit"""
        logger.info("Running pipeline once")
        result = self.run_single_cycle()
        self.cleanup()
        return result
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources")
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


