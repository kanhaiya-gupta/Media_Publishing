"""
S3/MinIO Loader
===============

Load data to S3/MinIO object storage.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, date_format
from datetime import datetime

from ..base.loader import BaseLoader
from ..utils.config import ETLConfig, get_etl_config
from ..utils.loader_config import get_table_load_config

import logging

logger = logging.getLogger(__name__)


class S3Loader(BaseLoader):
    """
    Load data to S3/MinIO object storage.
    
    Supports:
    - Parquet format
    - Delta Lake format
    - JSON format
    - Partitioned writes
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize S3 loader.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            loader_config: Loader-specific configuration
        """
        super().__init__(spark, loader_config)
        self.etl_config = config or get_etl_config()
        self.s3_config = self.etl_config.get_s3_config()
        
        # Configure Spark for S3/MinIO
        for key, value in self.s3_config.items():
            self.spark.conf.set(key, value)
    
    def load(
        self,
        df: DataFrame,
        destination: str,
        format: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        table_name: Optional[str] = None,
        s3_path_template: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Load data to S3/MinIO.
        
        Args:
            df: DataFrame to load
            destination: S3 path (s3a://bucket/path or relative path, or table name)
            format: File format ("parquet", "delta", "json", etc.)
            mode: Write mode ("overwrite", "append", "ignore", "error")
            partition_by: Columns to partition by (optional)
            table_name: Table name (used for path mapping if destination is table name)
            s3_path_template: S3 path template (e.g., "{domain}/{table}/{date}/")
            **kwargs: Additional format-specific options
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            if not self.validate_input(df):
                return False
            
            # Get table-specific config if table_name is provided
            if table_name:
                table_config = get_table_load_config(table_name, "s3")
                if table_config:
                    # Override with table-specific config
                    if table_config.format:
                        format = table_config.format
                    if table_config.mode:
                        mode = table_config.mode
                    if table_config.partition_by:
                        partition_by = table_config.partition_by
                    if table_config.s3_path_template:
                        s3_path_template = table_config.s3_path_template
            
            # Build S3 path from template if provided
            if s3_path_template and table_name:
                destination = self._build_s3_path(s3_path_template, table_name, df)
            elif not destination.startswith("s3a://") and not destination.startswith("s3://"):
                # If destination is a table name, try to get path from config
                if table_name:
                    table_config = get_table_load_config(table_name, "s3")
                    if table_config and table_config.s3_path_template:
                        destination = self._build_s3_path(
                            table_config.s3_path_template, table_name, df
                        )
                    else:
                        # Default path structure
                        destination = f"data-lake/{table_name}/"
                else:
                    # Use destination as-is
                    pass
            
            # Ensure path uses s3a:// protocol
            if destination.startswith("s3://"):
                destination = destination.replace("s3://", "s3a://", 1)
            elif not destination.startswith("s3a://"):
                # Assume it's a relative path, prepend bucket
                bucket = self.etl_config.s3_bucket
                destination = f"s3a://{bucket}/{destination}"
            
            self.log_load_stats(df, f"S3: {destination}")
            
            writer = df.write.format(format).mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            # Apply format-specific options
            for key, value in kwargs.items():
                writer = writer.option(key, value)
            
            writer.save(destination)
            
            self.logger.info(f"Successfully loaded data to S3: {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to S3: {e}", exc_info=True)
            return False
    
    def _build_s3_path(
        self,
        template: str,
        table_name: str,
        df: DataFrame
    ) -> str:
        """
        Build S3 path from template.
        
        Args:
            template: Path template (e.g., "{domain}/{table}/{date}/")
            table_name: Table name
            df: DataFrame (used to extract date for partitioning)
            
        Returns:
            S3 path string
        """
        # Extract domain from table name
        domain = table_name.split("_")[0] if "_" in table_name else "default"
        
        # Try to extract date from DataFrame
        date_str = None
        date_columns = [
            "event_date", "created_at", "updated_at", "date", "session_start",
            "performance_date", "engagement_date", "analytics_date", "period_start",
            "prediction_date", "usage_date", "check_timestamp", "alert_timestamp",
            "validation_timestamp", "changed_at", "discovered_at", "anonymization_date",
            "execution_started_at", "training_started_at", "prediction_timestamp"
        ]
        for col_name in date_columns:
            if col_name in df.columns:
                try:
                    # Get first non-null date value
                    sample_date = df.select(col_name).filter(
                        col(col_name).isNotNull()
                    ).limit(1).collect()
                    if sample_date:
                        date_val = sample_date[0][col_name]
                        if date_val:
                            # Format date as YYYY-MM-DD
                            if hasattr(date_val, 'strftime'):
                                date_str = date_val.strftime("%Y-%m-%d")
                            else:
                                date_str = str(date_val)[:10]  # Take first 10 chars
                            break
                except Exception as e:
                    logger.debug(f"Could not extract date from {col_name}: {e}")
                    continue
        
        # If no date found, use current date
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Replace template variables
        path = template.replace("{domain}", domain)
        path = path.replace("{table}", table_name)
        path = path.replace("{date}", date_str)
        
        return path

