"""
Base Loader
===========

Abstract base class for data loaders.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """
    Abstract base class for data loaders.
    
    Loaders are responsible for writing data to various destinations
    (PostgreSQL, ClickHouse, S3, Kafka, etc.).
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Loader-specific configuration
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def load(self, df: DataFrame, destination: str, **kwargs) -> bool:
        """
        Load data to destination.
        
        Args:
            df: Spark DataFrame to load
            destination: Destination identifier (table name, path, topic, etc.)
            **kwargs: Load-specific parameters
            
        Returns:
            True if load successful, False otherwise
        """
        pass
    
    def validate_input(self, df: DataFrame) -> bool:
        """
        Validate that input DataFrame is not empty.
        
        Args:
            df: Input DataFrame
            
        Returns:
            True if DataFrame is valid, False otherwise
        """
        try:
            row_count = df.count()
            if row_count == 0:
                self.logger.warning("Input DataFrame is empty")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error validating input DataFrame: {e}")
            return False
    
    def log_load_stats(self, df: DataFrame, destination: str):
        """
        Log load statistics.
        
        Args:
            df: DataFrame being loaded
            destination: Destination identifier
        """
        try:
            row_count = df.count()
            column_count = len(df.columns)
            self.logger.info(
                f"Loading {row_count} rows, {column_count} columns to {destination}"
            )
        except Exception as e:
            self.logger.warning(f"Could not log load stats: {e}")

