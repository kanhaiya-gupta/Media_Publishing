"""
Base Transformer
================

Base transformer class with common transformation utilities.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, lit, current_timestamp

from ..base.transformer import BaseTransformer

import logging

logger = logging.getLogger(__name__)


class BaseDataTransformer(BaseTransformer):
    """
    Base transformer with common data transformation utilities.
    
    Provides common transformations:
    - Null handling
    - Data type conversions
    - Column renaming
    - Data cleaning
    
    Can automatically delegate to domain-specific transformers if table/topic name is provided.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Transformer-specific configuration
                - table_name: Name of table (for automatic transformer selection)
                - topic_name: Name of topic (for automatic transformer selection)
        """
        super().__init__(spark, config)
        self.table_name = self.config.get("table_name")
        self.topic_name = self.config.get("topic_name")
        self._delegate_transformer = None
        
        # Only auto-select delegate transformer if this is BaseDataTransformer itself
        # (not a subclass like PydanticModelTransformer, which already has specific logic)
        # This prevents infinite recursion when subclasses call super().__init__()
        if (self.table_name or self.topic_name) and self.__class__ == BaseDataTransformer:
            name = self.table_name or self.topic_name
            # Lazy import to avoid circular dependency
            from ..utils.transformer_factory import TransformerFactory
            self._delegate_transformer = TransformerFactory.get_transformer(
                spark,
                name,
                config
            )
            # Only log if we got a different transformer (not BaseDataTransformer)
            if self._delegate_transformer.__class__ != BaseDataTransformer:
                logger.info(
                    f"Auto-selected {self._delegate_transformer.__class__.__name__} "
                    f"for '{name}'"
                )
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform the input DataFrame.
        
        If a domain-specific transformer was auto-selected, delegates to it.
        Otherwise, performs basic transformation (pass-through by default).
        
        Args:
            df: Input DataFrame
            **kwargs: Transform parameters
                - table_name: Name of table (for automatic transformer selection)
                - topic_name: Name of topic (for automatic transformer selection)
                - path_name: Name of path (for automatic transformer selection)
            
        Returns:
            Transformed DataFrame
        """
        # Check if table/topic/path name is provided in kwargs (runtime selection)
        table_name = kwargs.get("table_name") or self.table_name
        topic_name = kwargs.get("topic_name") or self.topic_name
        path_name = kwargs.get("path_name")
        
        # If runtime name provided and no delegate yet, create one
        # Only do this if this is BaseDataTransformer itself (not a subclass)
        # This prevents infinite recursion when subclasses call super().transform()
        if (table_name or topic_name or path_name) and not self._delegate_transformer and self.__class__ == BaseDataTransformer:
            name = table_name or topic_name or path_name
            # Lazy import to avoid circular dependency
            from ..utils.transformer_factory import TransformerFactory
            # Create a fresh config without table_name to avoid conflicts
            # The factory will set the correct table_name for the delegate transformer
            delegate_config = {k: v for k, v in (self.config or {}).items() 
                             if k not in ["table_name", "topic_name", "path_name"]}
            self._delegate_transformer = TransformerFactory.get_transformer(
                self.spark,
                name,
                delegate_config
            )
            if self._delegate_transformer.__class__ != BaseDataTransformer:
                logger.info(
                    f"Auto-selected {self._delegate_transformer.__class__.__name__} "
                    f"for '{name}'"
                )
        
        # Delegate to domain-specific transformer if available
        if self._delegate_transformer and self._delegate_transformer.__class__ != BaseDataTransformer:
            # Remove table/topic/path_name from kwargs before passing to delegate
            delegate_kwargs = {k: v for k, v in kwargs.items() 
                             if k not in ["table_name", "topic_name", "path_name"]}
            return self._delegate_transformer.transform(df, **delegate_kwargs)
        
        # Otherwise, perform basic transformation (pass-through by default)
        return df
    
    def clean_nulls(self, df: DataFrame, columns: Optional[list] = None, fill_value: Any = None) -> DataFrame:
        """
        Clean null values in specified columns.
        
        Args:
            df: Input DataFrame
            columns: Columns to clean (None for all columns)
            fill_value: Value to fill nulls with
            
        Returns:
            DataFrame with cleaned nulls
        """
        if columns is None:
            columns = df.columns
        
        row_count_before = df.count()
        logger.debug(f"clean_nulls: Before cleaning - {row_count_before} rows, columns to clean: {columns}")
        logger.debug(f"clean_nulls: DataFrame columns: {df.columns}")
        
        result_df = df
        missing_columns = []
        for col_name in columns:
            if col_name in df.columns:
                if fill_value is not None:
                    result_df = result_df.withColumn(
                        col_name,
                        when(col(col_name).isNull(), lit(fill_value)).otherwise(col(col_name))
                    )
                else:
                    # Drop nulls
                    null_count = result_df.filter(col(col_name).isNull()).count()
                    if null_count > 0:
                        logger.debug(f"clean_nulls: Dropping {null_count} rows with null in column '{col_name}'")
                    result_df = result_df.filter(col(col_name).isNotNull())
            else:
                missing_columns.append(col_name)
                logger.warning(f"clean_nulls: Column '{col_name}' not found in DataFrame. Available columns: {df.columns}")
                # IMPORTANT: Skip missing columns - don't filter on them!
                # If we filter on a missing column, it will filter out ALL rows
                logger.warning(f"clean_nulls: Skipping column '{col_name}' as it doesn't exist in DataFrame")
        
        if missing_columns:
            logger.warning(f"clean_nulls: Missing columns: {missing_columns}. These columns were skipped (not filtered).")
            logger.warning(f"clean_nulls: This may indicate a mismatch between expected columns and actual DataFrame columns.")
            logger.warning(f"clean_nulls: Expected columns: {columns}")
            logger.warning(f"clean_nulls: Actual DataFrame columns: {df.columns}")
        
        row_count_after = result_df.count()
        logger.debug(f"clean_nulls: After cleaning - {row_count_after} rows (removed {row_count_before - row_count_after} rows)")
        
        if row_count_after == 0 and row_count_before > 0:
            logger.error(f"clean_nulls: All rows were filtered out! Before: {row_count_before} rows, After: {row_count_after} rows")
            logger.error(f"clean_nulls: This likely means required columns are missing or all have nulls")
        
        return result_df
    
    def rename_columns(self, df: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
        """
        Rename columns in DataFrame.
        
        Args:
            df: Input DataFrame
            column_mapping: Dictionary mapping old names to new names
            
        Returns:
            DataFrame with renamed columns
        """
        result_df = df
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                result_df = result_df.withColumnRenamed(old_name, new_name)
        
        return result_df
    
    def add_timestamp(self, df: DataFrame, column_name: str = "processed_at") -> DataFrame:
        """
        Add processing timestamp column.
        
        Args:
            df: Input DataFrame
            column_name: Name of timestamp column
            
        Returns:
            DataFrame with timestamp column
        """
        return df.withColumn(column_name, current_timestamp())
    
    def filter_rows(self, df: DataFrame, condition: str) -> DataFrame:
        """
        Filter rows based on condition.
        
        Args:
            df: Input DataFrame
            condition: SQL condition string
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(condition)

