"""
MinIO Client Module
Handles all S3-compatible storage operations using MinIO
"""

import os
from typing import List, Optional
from minio import Minio
from minio.error import S3Error
import logging

logger = logging.getLogger(__name__)


class MinIOClient:
    """Client for interacting with MinIO/S3-compatible storage"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 secure: bool = False, bucket_name: str = "samsung-data"):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO server endpoint (e.g., "localhost:9000")
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Use HTTPS if True
            bucket_name: Name of the bucket to use
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket {self.bucket_name} already exists")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    def list_files(self, prefix: str, recursive: bool = True) -> List[str]:
        """
        List all files with the given prefix
        
        Args:
            prefix: Prefix to filter files
            recursive: Whether to list recursively
            
        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            return []
    
    def upload_file(self, local_path: str, object_name: str) -> bool:
        """
        Upload a local file to MinIO
        
        Args:
            local_path: Path to local file
            object_name: Object name in MinIO
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.fput_object(
                self.bucket_name,
                object_name,
                local_path
            )
            logger.info(f"Uploaded {local_path} to {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False
    
    def download_file(self, object_name: str, local_path: str) -> bool:
        """
        Download a file from MinIO to local filesystem
        
        Args:
            object_name: Object name in MinIO
            local_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.client.fget_object(
                self.bucket_name,
                object_name,
                local_path
            )
            logger.info(f"Downloaded {object_name} to {local_path}")
            return True
        except S3Error as e:
            logger.error(f"Error downloading {object_name}: {e}")
            return False
    
    def move_file(self, source_object: str, dest_object: str) -> bool:
        """
        Move a file within MinIO (copy + delete)
        
        Args:
            source_object: Source object name
            dest_object: Destination object name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Copy object
            from minio.commonconfig import CopySource
            self.client.copy_object(
                self.bucket_name,
                dest_object,
                CopySource(self.bucket_name, source_object)
            )
            # Delete source
            self.client.remove_object(self.bucket_name, source_object)
            logger.info(f"Moved {source_object} to {dest_object}")
            return True
        except S3Error as e:
            logger.error(f"Error moving {source_object} to {dest_object}: {e}")
            return False
    
    def delete_file(self, object_name: str) -> bool:
        """
        Delete a file from MinIO
        
        Args:
            object_name: Object name to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.remove_object(self.bucket_name, object_name)
            logger.info(f"Deleted {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting {object_name}: {e}")
            return False
    
    def file_exists(self, object_name: str) -> bool:
        """
        Check if a file exists in MinIO
        
        Args:
            object_name: Object name to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error:
            return False
    
    def get_file_size(self, object_name: str) -> Optional[int]:
        """
        Get the size of a file in MinIO
        
        Args:
            object_name: Object name
            
        Returns:
            File size in bytes, or None if file doesn't exist
        """
        try:
            stat = self.client.stat_object(self.bucket_name, object_name)
            return stat.size
        except S3Error as e:
            logger.error(f"Error getting file size for {object_name}: {e}")
            return None




