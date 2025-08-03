"""
Storage backends for saving map tiles.
Supports local filesystem and MinIO object storage.
"""
import io
import os
import logging
from typing import List, Optional, Dict, Any, BinaryIO
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    logger.warning("MinIO client not available. Install with: pip install minio")

class StorageBackend:
    """Base class for storage backends."""
    
    def save(self, data: bytes, path: str) -> bool:
        """Save data to the storage backend.
        
        Args:
            data: Data to save
            path: Path to save the data to
            
        Returns:
            bool: True if successful, False otherwise
        """
        raise NotImplementedError("Subclasses must implement save()")
    
    def exists(self, path: str) -> bool:
        """Check if a file exists in the storage backend.
        
        Args:
            path: Path to check
            
        Returns:
            bool: True if the file exists, False otherwise
        """
        raise NotImplementedError("Subclasses must implement exists()")
    
    def list_files(self, prefix: str = '') -> List[str]:
        """List files in the storage backend.
        
        Args:
            prefix: Optional prefix to filter files
            
        Returns:
            List of file paths
        """
        raise NotImplementedError("Subclasses must implement list_files()")


class LocalStorage(StorageBackend):
    """Local filesystem storage backend."""
    
    def __init__(self, base_path: str):
        """Initialize local storage.
        
        Args:
            base_path: Base path for all files
        """
        self.base_path = os.path.abspath(base_path)
        os.makedirs(self.base_path, exist_ok=True)
        logger.info(f"Initialized local storage at {self.base_path}")
    
    def save(self, data: bytes, path: str) -> bool:
        """Save data to a local file."""
        try:
            full_path = os.path.join(self.base_path, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            with open(full_path, 'wb') as f:
                f.write(data)
            
            logger.debug(f"Saved {len(data)} bytes to {full_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving file {path}: {e}")
            return False
    
    def exists(self, path: str) -> bool:
        """Check if a file exists locally."""
        full_path = os.path.join(self.base_path, path)
        return os.path.exists(full_path)
    
    def list_files(self, prefix: str = '') -> List[str]:
        """List files in the local directory."""
        result = []
        base = os.path.join(self.base_path, prefix) if prefix else self.base_path
        
        for root, _, files in os.walk(base):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, self.base_path)
                result.append(rel_path)
        
        return result


class MinIOStorage(StorageBackend):
    """MinIO object storage backend."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 bucket_name: str, secure: bool = True, region: str = None):
        """Initialize MinIO storage.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            bucket_name: Name of the bucket to use
            secure: Whether to use HTTPS
            region: AWS region (optional)
        """
        if not MINIO_AVAILABLE:
            raise RuntimeError("MinIO client is not available. Install with: pip install minio")
        
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
        self.bucket_name = bucket_name
        self.ensure_bucket_exists()
        logger.info(f"Initialized MinIO storage at {endpoint}/{bucket_name}")
    
    def ensure_bucket_exists(self):
        """Ensure the bucket exists, create it if it doesn't."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    def save(self, data: bytes, path: str) -> bool:
        """Save data to MinIO."""
        try:
            # Ensure path doesn't start with /
            path = path.lstrip('/')
            
            # Upload the data
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=path,
                data=io.BytesIO(data),
                length=len(data)
            )
            
            logger.debug(f"Uploaded {len(data)} bytes to {self.bucket_name}/{path}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading to MinIO: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading to MinIO: {e}")
            return False
    
    def exists(self, path: str) -> bool:
        """Check if an object exists in MinIO."""
        try:
            path = path.lstrip('/')
            self.client.stat_object(self.bucket_name, path)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            logger.error(f"Error checking if object exists in MinIO: {e}")
            return False
    
    def list_files(self, prefix: str = '') -> List[str]:
        """List objects in the MinIO bucket."""
        try:
            objects = self.client.list_objects(
                bucket_name=self.bucket_name,
                prefix=prefix,
                recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects in MinIO: {e}")
            return []


def create_storage(config: Dict[str, Any]) -> StorageBackend:
    """Create a storage backend from configuration.
    
    Args:
        config: Storage configuration dictionary
        
    Returns:
        Initialized storage backend
    """
    storage_type = config.get('type')
    
    if storage_type == 'local':
        return LocalStorage(config.get('path', './output'))
    elif storage_type == 'minio':
        if not MINIO_AVAILABLE:
            logger.warning("MinIO client not available. Falling back to local storage.")
            return LocalStorage(config.get('path', './output'))
        
        return MinIOStorage(
            endpoint=config['endpoint'],
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            bucket_name=config['bucket_name'],
            secure=config.get('secure', True),
            region=config.get('region')
        )
    else:
        logger.warning(f"Unknown storage type: {storage_type}. Using local storage.")
        return LocalStorage(config.get('path', './output'))
