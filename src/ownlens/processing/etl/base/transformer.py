"""
Base Transformer
================

Abstract base class for data transformers.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.
    
    Transformers are responsible for cleaning, enriching, aggregating,
    and transforming data according to business rules.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Transformer-specific configuration
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform the input DataFrame.
        
        Args:
            df: Input Spark DataFrame
            **kwargs: Transform-specific parameters
            
        Returns:
            Transformed Spark DataFrame
        """
        pass
    
    def validate_input(self, df: DataFrame, required_columns: list) -> bool:
        """
        Validate that input DataFrame has required columns.
        
        Args:
            df: Input DataFrame
            required_columns: List of required column names
            
        Returns:
            True if all required columns are present, False otherwise
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        return True
    
    def log_transformation_stats(self, input_df: DataFrame, output_df: DataFrame, transformation: str):
        """
        Log transformation statistics.
        
        Args:
            input_df: Input DataFrame
            output_df: Output DataFrame
            transformation: Transformation identifier
        """
        try:
            input_rows = input_df.count()
            output_rows = output_df.count()
            input_cols = len(input_df.columns)
            output_cols = len(output_df.columns)
            
            self.logger.info(
                f"Transformation '{transformation}': "
                f"Input: {input_rows} rows, {input_cols} cols -> "
                f"Output: {output_rows} rows, {output_cols} cols"
            )
        except Exception as e:
            self.logger.warning(f"Could not log transformation stats: {e}")

