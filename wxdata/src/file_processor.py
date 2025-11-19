"""
File Processor Module
Handles file detection, format identification, and processing coordination
"""

import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logger = logging.getLogger(__name__)


class FileProcessor:
    """Processes files and coordinates ingestion"""
    
    def __init__(self, minio_client, schema_manager, iceberg_manager):
        """
        Initialize file processor
        
        Args:
            minio_client: MinIOClient instance
            schema_manager: SchemaManager instance
            iceberg_manager: IcebergManager instance
        """
        self.minio_client = minio_client
        self.schema_manager = schema_manager
        self.iceberg_manager = iceberg_manager
        self.processed_files = set()  # Track processed files to avoid duplicates
        self.processed_files_lock = threading.Lock()  # Lock for thread-safe access to processed_files
    
    def detect_new_files(self, landing_prefix: str) -> List[str]:
        """
        Detect new CSV files in the landing zone
        
        Args:
            landing_prefix: Prefix for landing zone (e.g., "landing/Samsung")
            
        Returns:
            List of new file paths
        """
        try:
            all_files = self.minio_client.list_files(landing_prefix, recursive=True)
            csv_files = [f for f in all_files if f.endswith('.csv')]
            
            # Filter out already processed files (thread-safe)
            with self.processed_files_lock:
                new_files = [f for f in csv_files if f not in self.processed_files]
            
            logger.info(f"Detected {len(new_files)} new CSV files out of {len(csv_files)} total")
            return new_files
        except Exception as e:
            logger.error(f"Error detecting new files: {e}")
            return []
    
    def identify_format(self, file_path: str) -> Optional[str]:
        """
        Identify the format type of a file
        
        Args:
            file_path: Path to the file
            
        Returns:
            Format type (carrier or du) or None
        """
        return self.schema_manager.identify_format(file_path)
    
    def process_file(self, file_path: str, format_type: str, 
                    table_config: Dict, s3_base_path: str) -> bool:
        """
        Process a single CSV file
        
        Args:
            file_path: Path to the file in MinIO
            format_type: Format type (carrier or du)
            table_config: Table configuration from config
            s3_base_path: Base S3 path for constructing full paths
            
        Returns:
            True if successful, False otherwise
        """
        try:
            table_name = table_config["table_name"]
            partition_columns = table_config.get("partition_columns", [])
            
            # Get schema for this format
            schema = self.schema_manager.get_spark_schema(format_type)
            
            # Ensure table exists
            if not self.iceberg_manager.table_exists(table_name):
                logger.info(f"Table {table_name} does not exist, creating it...")
                self.iceberg_manager.create_table(
                    table_name,
                    schema,
                    partition_columns
                )
            
            # Construct full S3 path
            s3_path = f"s3a://{self.minio_client.bucket_name}/{file_path}"
            
            # Load CSV into Iceberg table
            self.iceberg_manager.load_csv_to_table(
                s3_path,
                table_name,
                schema,
                format_type
            )
            
            logger.info(f"Successfully processed file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return False
    
    def move_to_processed(self, file_path: str, processed_prefix: str) -> bool:
        """
        Move a file to the processed directory
        
        Args:
            file_path: Path to the file in MinIO
            processed_prefix: Prefix for processed files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract relative path from landing zone
            if "landing/" in file_path:
                relative_path = file_path.split("landing/", 1)[1]
            else:
                relative_path = os.path.basename(file_path)
            
            # Construct processed path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.basename(file_path)
            name, ext = os.path.splitext(filename)
            processed_filename = f"{name}_{timestamp}{ext}"
            dest_path = f"{processed_prefix}/{relative_path.replace(os.path.basename(file_path), processed_filename)}"
            
            # Ensure directory structure exists
            dest_dir = os.path.dirname(dest_path)
            if not dest_dir.endswith("/"):
                dest_dir += "/"
            
            # Move file
            success = self.minio_client.move_file(file_path, dest_path)
            
            if success:
                # Thread-safe update of processed_files
                with self.processed_files_lock:
                    self.processed_files.add(file_path)
                logger.info(f"Moved {file_path} to {dest_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error moving file {file_path} to processed: {e}")
            return False
    
    def _process_single_file(self, file_path: str, table_configs: Dict[str, Dict],
                             processed_prefix: str, s3_base_path: str) -> Tuple[bool, str]:
        """
        Process a single file (used for parallel processing)
        
        Args:
            file_path: Path to the file to process
            table_configs: Dictionary mapping format types to table configs
            processed_prefix: Processed zone prefix
            s3_base_path: Base S3 path
            
        Returns:
            Tuple of (success: bool, error_message: str)
        """
        try:
            # Identify format
            format_type = self.identify_format(file_path)
            
            if not format_type:
                error_msg = f"Could not identify format for {file_path}"
                logger.warning(error_msg)
                return False, error_msg
            
            if format_type not in table_configs:
                error_msg = f"No table config for format {format_type}, skipping {file_path}"
                logger.warning(error_msg)
                return False, error_msg
            
            # Process file
            table_config = table_configs[format_type]
            if self.process_file(file_path, format_type, table_config, s3_base_path):
                # Move to processed
                if self.move_to_processed(file_path, processed_prefix):
                    return True, ""
                else:
                    logger.warning(f"File processed but failed to move: {file_path}")
                    return True, ""  # Still count as successful
            else:
                error_msg = f"Failed to process file: {file_path}"
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Exception processing {file_path}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def process_batch(self, files: List[str], table_configs: Dict[str, Dict],
                     landing_prefix: str, processed_prefix: str, 
                     s3_base_path: str, max_workers: int = 10) -> Tuple[int, int]:
        """
        Process a batch of files in parallel
        
        Args:
            files: List of file paths to process
            table_configs: Dictionary mapping format types to table configs
            landing_prefix: Landing zone prefix
            processed_prefix: Processed zone prefix
            s3_base_path: Base S3 path
            max_workers: Maximum number of parallel workers (default: 10)
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not files:
            return 0, 0
        
        successful = 0
        failed = 0
        
        # Use ThreadPoolExecutor for parallel processing
        # ThreadPoolExecutor is suitable for I/O-bound operations like MinIO and Spark operations
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(
                    self._process_single_file,
                    file_path,
                    table_configs,
                    processed_prefix,
                    s3_base_path
                ): file_path
                for file_path in files
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success, error_msg = future.result()
                    if success:
                        successful += 1
                    else:
                        failed += 1
                        if error_msg:
                            logger.debug(f"File processing failed: {error_msg}")
                except Exception as e:
                    failed += 1
                    logger.error(f"Exception in future for {file_path}: {e}", exc_info=True)
        
        logger.info(f"Batch processing complete: {successful} successful, {failed} failed")
        return successful, failed
    
    def _process_table_group(self, format_type: str, file_list: List[str], 
                            table_config: Dict, processed_prefix: str) -> Tuple[int, int]:
        """
        Process a single table group (format type) - loads all files for the table
        in a single Spark job.
        
        Args:
            format_type: Format type (carrier or du)
            file_list: List of file paths for this format/table
            table_config: Table configuration
            processed_prefix: Processed zone prefix
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        try:
            table_name = table_config["table_name"]
            partition_columns = table_config.get("partition_columns", [])
            
            # Get schema for this format
            schema = self.schema_manager.get_spark_schema(format_type)
            
            # Ensure table exists
            if not self.iceberg_manager.table_exists(table_name):
                logger.info(f"Table {table_name} does not exist, creating it...")
                self.iceberg_manager.create_table(
                    table_name,
                    schema,
                    partition_columns
                )
            
            # Construct full S3 paths for all files
            s3_paths = [f"s3a://{self.minio_client.bucket_name}/{file_path}" 
                       for file_path in file_list]
            
            # Load all CSV files for this table in a single Spark job
            self.iceberg_manager.load_csv_files_to_table(
                s3_paths,
                table_name,
                schema,
                format_type
            )
            
            # Move all files to processed
            successful = 0
            failed = 0
            for file_path in file_list:
                if self.move_to_processed(file_path, processed_prefix):
                    successful += 1
                else:
                    failed += 1
                    logger.warning(f"File processed but failed to move: {file_path}")
            
            logger.info(f"Successfully processed {len(file_list)} files for table {table_name}")
            return successful, failed
            
        except Exception as e:
            logger.error(f"Error processing {len(file_list)} files for format {format_type}: {e}", 
                       exc_info=True)
            return 0, len(file_list)
    
    def process_batch_grouped_by_table(self, files: List[str], table_configs: Dict[str, Dict],
                                      landing_prefix: str, processed_prefix: str, 
                                      s3_base_path: str, max_workers: int = 10) -> Tuple[int, int]:
        """
        Process a batch of files grouped by table - loads all files for each table
        in a single Spark job for better performance. Processes multiple table groups
        in parallel using threads.
        
        Args:
            files: List of file paths to process
            table_configs: Dictionary mapping format types to table configs
            landing_prefix: Landing zone prefix
            processed_prefix: Processed zone prefix
            s3_base_path: Base S3 path
            max_workers: Maximum number of parallel workers (threads) for processing table groups
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not files:
            return 0, 0
        
        # Group files by format type (which determines the table)
        files_by_format: Dict[str, List[str]] = {}
        files_with_errors: List[str] = []
        
        for file_path in files:
            format_type = self.identify_format(file_path)
            if not format_type:
                logger.warning(f"Could not identify format for {file_path}, skipping")
                files_with_errors.append(file_path)
                continue
            
            if format_type not in table_configs:
                logger.warning(f"No table config for format {format_type}, skipping {file_path}")
                files_with_errors.append(file_path)
                continue
            
            if format_type not in files_by_format:
                files_by_format[format_type] = []
            files_by_format[format_type].append(file_path)
        
        total_successful = 0
        total_failed = len(files_with_errors)  # Start with files that had errors during grouping
        
        # Process each format/table group in parallel using ThreadPoolExecutor
        # Each thread will run a Spark job for its table group
        num_groups = len(files_by_format)
        logger.info(f"Processing {num_groups} table groups in parallel (max_workers: {max_workers})")
        
        with ThreadPoolExecutor(max_workers=min(max_workers, num_groups)) as executor:
            # Submit all table group processing tasks
            future_to_format = {
                executor.submit(
                    self._process_table_group,
                    format_type,
                    file_list,
                    table_configs[format_type],
                    processed_prefix
                ): format_type
                for format_type, file_list in files_by_format.items()
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_format):
                format_type = future_to_format[future]
                try:
                    successful, failed = future.result()
                    total_successful += successful
                    total_failed += failed
                except Exception as e:
                    # Count all files in this group as failed
                    failed_count = len(files_by_format[format_type])
                    total_failed += failed_count
                    logger.error(f"Exception processing table group {format_type}: {e}", exc_info=True)
        
        logger.info(f"Batch processing (grouped by table, parallel) complete: {total_successful} successful, {total_failed} failed")
        return total_successful, total_failed
    
    def should_compact(self, table_name: str, threshold: int = 10) -> bool:
        """
        Check if table should be compacted based on number of files
        
        Args:
            table_name: Name of the table
            threshold: Number of files threshold
            
        Returns:
            True if compaction should be performed
        """
        try:
            # This is a simplified check - in production, you might want to
            # check actual file count in the table
            # For now, we'll use a simple heuristic
            return False  # Can be enhanced with actual file count logic
        except Exception as e:
            logger.error(f"Error checking compaction status: {e}")
            return False


