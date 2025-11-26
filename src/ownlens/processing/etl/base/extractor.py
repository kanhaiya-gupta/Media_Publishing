"""
Base Extractor
==============

Abstract base class for data extractors.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.
    
    Extractors are responsible for reading data from various sources
    (PostgreSQL, Kafka, APIs, S3, etc.) and returning Spark DataFrames.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Extractor-specific configuration
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def extract(self, **kwargs) -> DataFrame:
        """
        Extract data from source.
        
        Args:
            **kwargs: Extract-specific parameters
            
        Returns:
            Spark DataFrame with extracted data
        """
        pass
    
    def validate_config(self, required_keys: list) -> bool:
        """
        Validate that required configuration keys are present.
        
        Args:
            required_keys: List of required configuration keys
            
        Returns:
            True if all required keys are present, False otherwise
        """
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            self.logger.error(f"Missing required configuration keys: {missing_keys}")
            return False
        return True
    
    def log_extraction_stats(self, df: DataFrame, source: str):
        """
        Log extraction statistics.
        
        Args:
            df: Extracted DataFrame
            source: Source identifier
        """
        try:
            row_count = df.count()
            column_count = len(df.columns)
            self.logger.info(
                f"Extracted {row_count} rows, {column_count} columns from {source}"
            )
        except Exception as e:
            self.logger.warning(f"Could not log extraction stats: {e}")

