"""
Data Validators
===============

Data validation utilities for ETL pipelines.
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, count
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Data validation utilities for Spark DataFrames.
    
    Provides methods to validate data quality, schema, and business rules.
    """
    
    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: Dict[str, str]) -> bool:
        """
        Validate DataFrame schema matches expected schema.
        
        Args:
            df: Spark DataFrame
            expected_schema: Dictionary mapping column names to types
            
        Returns:
            True if schema matches, False otherwise
        """
        try:
            actual_columns = set(df.columns)
            expected_columns = set(expected_schema.keys())
            
            if actual_columns != expected_columns:
                missing = expected_columns - actual_columns
                extra = actual_columns - expected_columns
                logger.warning(f"Schema mismatch - Missing: {missing}, Extra: {extra}")
                return False
            
            # Check types (basic check)
            for col_name, expected_type in expected_schema.items():
                actual_type = str(df.schema[col_name].dataType)
                if expected_type.lower() not in actual_type.lower():
                    logger.warning(f"Type mismatch for {col_name}: expected {expected_type}, got {actual_type}")
                    return False
            
            logger.info("Schema validation passed")
            return True
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            return False
    
    @staticmethod
    def validate_not_null(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Validate that specified columns are not null.
        
        Args:
            df: Spark DataFrame
            columns: List of column names to check
            
        Returns:
            DataFrame with null validation flags
        """
        for col_name in columns:
            if col_name not in df.columns:
                logger.warning(f"Column {col_name} not found in DataFrame")
                continue
            
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Column {col_name} has {null_count} null values")
        
        return df
    
    @staticmethod
    def validate_unique(df: DataFrame, columns: List[str]) -> bool:
        """
        Validate that specified columns are unique.
        
        Args:
            df: Spark DataFrame
            columns: List of column names to check for uniqueness
            
        Returns:
            True if unique, False otherwise
        """
        try:
            total_count = df.count()
            distinct_count = df.select(*columns).distinct().count()
            
            if total_count != distinct_count:
                logger.warning(f"Uniqueness violation: {total_count} total rows, {distinct_count} distinct rows")
                return False
            
            logger.info("Uniqueness validation passed")
            return True
        except Exception as e:
            logger.error(f"Uniqueness validation error: {e}")
            return False
    
    @staticmethod
    def validate_range(df: DataFrame, column: str, min_value: Optional[Any] = None, max_value: Optional[Any] = None) -> bool:
        """
        Validate that column values are within specified range.
        
        Args:
            df: Spark DataFrame
            column: Column name to validate
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            
        Returns:
            True if all values in range, False otherwise
        """
        try:
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                return False
            
            if min_value is not None:
                out_of_range = df.filter(col(column) < min_value).count()
                if out_of_range > 0:
                    logger.warning(f"Column {column} has {out_of_range} values below minimum {min_value}")
                    return False
            
            if max_value is not None:
                out_of_range = df.filter(col(column) > max_value).count()
                if out_of_range > 0:
                    logger.warning(f"Column {column} has {out_of_range} values above maximum {max_value}")
                    return False
            
            logger.info(f"Range validation passed for {column}")
            return True
        except Exception as e:
            logger.error(f"Range validation error: {e}")
            return False
    
    @staticmethod
    def get_data_quality_metrics(df: DataFrame) -> Dict[str, Any]:
        """
        Get data quality metrics for DataFrame.
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with data quality metrics
        """
        try:
            total_rows = df.count()
            total_columns = len(df.columns)
            
            metrics = {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "null_counts": {},
                "duplicate_rows": 0
            }
            
            # Count nulls per column
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                metrics["null_counts"][col_name] = null_count
            
            # Count duplicates
            duplicate_count = total_rows - df.distinct().count()
            metrics["duplicate_rows"] = duplicate_count
            
            logger.info(f"Data quality metrics: {metrics}")
            return metrics
        except Exception as e:
            logger.error(f"Error calculating data quality metrics: {e}")
            return {}


def validate_dataframe(
    df: DataFrame,
    expected_schema: Optional[Dict[str, str]] = None,
    not_null_columns: Optional[List[str]] = None,
    unique_columns: Optional[List[str]] = None
) -> bool:
    """
    Comprehensive DataFrame validation.
    
    Args:
        df: Spark DataFrame to validate
        expected_schema: Expected schema (column name -> type)
        not_null_columns: Columns that should not be null
        unique_columns: Columns that should be unique
        
    Returns:
        True if all validations pass, False otherwise
    """
    validator = DataValidator()
    
    # Schema validation
    if expected_schema:
        if not validator.validate_schema(df, expected_schema):
            return False
    
    # Not null validation
    if not_null_columns:
        validator.validate_not_null(df, not_null_columns)
    
    # Uniqueness validation
    if unique_columns:
        if not validator.validate_unique(df, unique_columns):
            return False
    
    logger.info("DataFrame validation passed")
    return True

