"""
S3/MinIO Extractor
==================

Extract data from S3/MinIO object storage.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import lit

from ..base.extractor import BaseExtractor
from ..utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


# Comprehensive list of S3 paths (relative to bucket root)
# These paths are used for media storage and content backups
# Note: Paths are relative to the bucket, not absolute S3 paths
S3_PATHS = [
    "",              # Bucket root (empty string = root)
    "images/",       # Image files
    "videos/",       # Video files
    "documents/",    # Document files
    "audio/",        # Audio files
    "backups/",      # Content backups
]


class S3Extractor(BaseExtractor):
    """
    Extract data from S3/MinIO object storage.
    
    Supports:
    - Parquet files
    - Delta Lake tables
    - JSON files
    - CSV files
    - Partitioned data
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize S3 extractor.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            extractor_config: Extractor-specific configuration
        """
        super().__init__(spark, extractor_config)
        self.etl_config = config or get_etl_config()
        self.s3_config = self.etl_config.get_s3_config()
        
        # Configure Spark for S3/MinIO
        for key, value in self.s3_config.items():
            self.spark.conf.set(key, value)
    
    def extract(
        self,
        path: str,
        format: str = "parquet",
        schema: Optional[StructType] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract data from S3/MinIO.
        
        For structured data (Parquet, JSON, CSV, etc.), reads the files directly.
        For binary media files (images, videos, etc.), extracts file metadata.
        
        Args:
            path: S3 path (s3a://bucket/path or relative path)
            format: File format ("parquet", "delta", "json", "csv", etc.)
            schema: Optional schema for schema inference
            **kwargs: Additional format-specific options
            
        Returns:
            Spark DataFrame with extracted data or file metadata
        """
        try:
            # Ensure path uses s3a:// protocol
            if path.startswith("s3://"):
                path = path.replace("s3://", "s3a://", 1)
            elif not path.startswith("s3a://"):
                # Assume it's a relative path, prepend bucket
                bucket = self.etl_config.s3_bucket
                # Handle empty path (bucket root)
                if path == "":
                    path = f"s3a://{bucket}/"
                else:
                    path = f"s3a://{bucket}/{path}"
            
            self.logger.info(f"Extracting data from S3: {path} (format: {format})")
            
            # Try to read as structured data first
            try:
                reader = self.spark.read.format(format)
                
                if schema:
                    reader = reader.schema(schema)
                
                # Disable partition inference for paths that might contain nested directories
                # This prevents Spark from treating nested UUID directories as partitions
                reader = reader.option("recursiveFileLookup", "true")
                reader = reader.option("basePath", path)  # Set base path to prevent partition inference
                
                # Apply format-specific options
                for key, value in kwargs.items():
                    reader = reader.option(key, value)
                
                df = reader.load(path)
                
                self.log_extraction_stats(df, f"S3: {path}")
                return df
                
            except Exception as read_error:
                error_msg = str(read_error)
                
                # Check if it's a path not found error
                if "PATH_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
                    # Path doesn't exist - return empty DataFrame
                    self.logger.warning(f"Path does not exist: {path}")
                    schema = StructType([
                        StructField("file_path", StringType(), False),
                        StructField("file_name", StringType(), False),
                        StructField("file_size", LongType(), False)
                    ])
                    return self.spark.createDataFrame([], schema)
                
                # Check if it's a partition inference error (nested directories treated as partitions)
                elif ("basePath" in error_msg.lower() or 
                      "partition" in error_msg.lower() or
                      "multiple root directories" in error_msg.lower()):
                    # This is likely binary media files with nested UUID directories
                    # Extract file metadata instead
                    self.logger.info(f"Path contains nested directories (likely binary media files), extracting file metadata instead...")
                    return self.extract_file_metadata(path)
                
                # Check if it's a binary media file error
                elif ("UNABLE_TO_INFER_SCHEMA" in error_msg or 
                      "Cannot infer schema" in error_msg or
                      "binary" in error_msg.lower()):
                    # This is likely binary media files (images, videos, etc.)
                    # Extract file metadata instead
                    self.logger.info(f"Path contains binary media files, extracting file metadata instead...")
                    return self.extract_file_metadata(path)
                else:
                    # Re-raise other errors
                    raise
            
        except Exception as e:
            self.logger.error(f"Error extracting data from S3: {e}", exc_info=True)
            raise
    
    def extract_file_metadata(self, path: str) -> DataFrame:
        """
        Extract file metadata (list files) from S3/MinIO path.
        Used for binary media files that cannot be read as structured data.
        
        Uses Hadoop FileSystem API (similar to check_minio_data.py) to list files.
        
        Args:
            path: S3 path (s3a://bucket/path)
            
        Returns:
            Spark DataFrame with file metadata (file_path, file_name, file_size)
        """
        try:
            # Use Hadoop FileSystem API to list files (similar to check_minio_data.py)
            self.logger.info(f"Listing files in {path}...")
            
            # Get Hadoop FileSystem via Java API
            jvm = self.spark.sparkContext._jvm
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            
            # Create URI and Path objects
            uri = jvm.java.net.URI(path)
            fs_path = jvm.org.apache.hadoop.fs.Path(path)
            
            # Get FileSystem instance
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            
            # List files recursively in the path
            # Files are stored as: images/{brand_id}/{media_id}/original.jpg
            try:
                # Use listFiles to recursively list all files
                # listFiles(path, recursive) - recursive=True lists all files in subdirectories
                try:
                    # Try recursive listing first (Hadoop 2.7+)
                    remote_iterator = fs.listFiles(fs_path, True)  # True = recursive
                    file_statuses = []
                    while remote_iterator.hasNext():
                        file_statuses.append(remote_iterator.next())
                except Exception:
                    # Fallback to non-recursive listing, then manually recurse
                    file_statuses = []
                    self._list_files_recursive(fs, fs_path, file_statuses)
                    
            except Exception as e:
                # Path might not exist or be empty
                self.logger.warning(f"Could not list files in {path}: {e}")
                schema = StructType([
                    StructField("file_path", StringType(), False),
                    StructField("file_name", StringType(), False),
                    StructField("file_size", LongType(), False)
                ])
                return self.spark.createDataFrame([], schema)
            
            # Build metadata list
            metadata_list = []
            for status in file_statuses:
                if status.isFile():  # Only include files, not directories
                    file_path = status.getPath().toString()
                    file_name = status.getPath().getName()
                    file_size = status.getLen()
                    
                    metadata_list.append({
                        "file_path": file_path,
                        "file_name": file_name,
                        "file_size": file_size
                    })
            
            # Create DataFrame from metadata
            if metadata_list:
                schema = StructType([
                    StructField("file_path", StringType(), False),
                    StructField("file_name", StringType(), False),
                    StructField("file_size", LongType(), False)
                ])
                
                df = self.spark.createDataFrame(metadata_list, schema)
                self.logger.info(f"Extracted metadata for {len(metadata_list)} files from {path}")
                return df
            else:
                # Return empty DataFrame with schema
                schema = StructType([
                    StructField("file_path", StringType(), False),
                    StructField("file_name", StringType(), False),
                    StructField("file_size", LongType(), False)
                ])
                df = self.spark.createDataFrame([], schema)
                self.logger.info(f"No files found in {path}")
                return df
                
        except Exception as e:
            self.logger.warning(f"Could not extract file metadata using Hadoop FileSystem API: {e}")
            # Return empty DataFrame with schema
            schema = StructType([
                StructField("file_path", StringType(), False),
                StructField("file_name", StringType(), False),
                StructField("file_size", LongType(), False)
            ])
            return self.spark.createDataFrame([], schema)
    
    def _list_files_recursive(self, fs, path, file_list):
        """
        Recursively list all files in a directory (fallback method).
        
        Args:
            fs: Hadoop FileSystem instance
            path: Hadoop Path object
            file_list: List to append FileStatus objects to
        """
        try:
            statuses = fs.listStatus(path)
            for status in statuses:
                if status.isFile():
                    file_list.append(status)
                elif status.isDirectory():
                    # Recursively list files in subdirectory
                    self._list_files_recursive(fs, status.getPath(), file_list)
        except Exception:
            # Ignore errors (path might not exist or be empty)
            pass
    
    def extract_parquet(self, path: str, **kwargs) -> DataFrame:
        """Extract Parquet files from S3."""
        return self.extract(path, format="parquet", **kwargs)
    
    def extract_delta(self, path: str, **kwargs) -> DataFrame:
        """Extract Delta Lake table from S3."""
        return self.extract(path, format="delta", **kwargs)
    
    def extract_json(self, path: str, **kwargs) -> DataFrame:
        """Extract JSON files from S3."""
        return self.extract(path, format="json", **kwargs)
    
    def extract_csv(self, path: str, header: bool = True, **kwargs) -> DataFrame:
        """Extract CSV files from S3."""
        return self.extract(
            path,
            format="csv",
            header=str(header).lower(),
            inferSchema="true",
            **kwargs
        )

