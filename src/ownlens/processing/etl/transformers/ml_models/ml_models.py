"""
ML Models Domain Transformers
==============================

All ML models-related transformers in one file.
Transform ML model data with Pydantic validation.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator
from .....models.ml_models.model_prediction import ModelPrediction
from .....models.ml_models.model_registry import ModelRegistry
from .....models.ml_models.model_feature import ModelFeature
from .....models.ml_models.training_run import TrainingRun
from .....models.ml_models.model_ab_test import ModelABTest
from .....models.ml_models.model_monitoring import ModelMonitoring

import logging

logger = logging.getLogger(__name__)


class ModelPredictionTransformer(BaseDataTransformer):
    """
    Transform model prediction data.
    
    Transformations:
    - Clean and validate model prediction data
    - Aggregate prediction metrics
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize model prediction transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ModelPrediction if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform model prediction data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with model prediction data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["prediction_id", "model_id", "prediction_type", "predicted_at"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df
    
    def aggregate_predictions(self, df: DataFrame, group_by: list) -> DataFrame:
        """
        Aggregate model prediction data by specified columns.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            
        Returns:
            Aggregated DataFrame
        """
        agg_exprs = [
            count("prediction_id").alias("prediction_count"),
            avg("prediction_confidence").alias("avg_confidence"),
            avg("prediction_probability").alias("avg_probability"),
            sum(spark_sum("is_correct").cast("int")).alias("correct_predictions"),
        ]
        
        return df.groupBy(*group_by).agg(*agg_exprs)


class ModelRegistryTransformer(BaseDataTransformer):
    """
    Transform model registry data.
    
    Transformations:
    - Clean and validate model registry data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize model registry transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ModelRegistry if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform model registry data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with model registry data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        required_fields = ["model_id", "model_name", "model_code", "model_version", "model_type", "domain", "algorithm", "company_id", "brand_id"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df


class ModelFeatureTransformer(BaseDataTransformer):
    """
    Transform model feature data.
    
    Transformations:
    - Clean and validate model feature data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize model feature transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ModelFeature if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform model feature data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with model feature data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["feature_id", "model_id", "feature_name", "feature_code", "feature_type"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df


class TrainingRunTransformer(BaseDataTransformer):
    """
    Transform training run data.
    
    Transformations:
    - Clean and validate training run data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize training run transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = TrainingRun if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform training run data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with training run data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["run_id", "model_id", "training_started_at", "training_status"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df


class ModelABTestTransformer(BaseDataTransformer):
    """
    Transform model AB test data.
    
    Transformations:
    - Clean and validate model AB test data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize model AB test transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ModelABTest if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform model AB test data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with model AB test data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        # Note: Model has test_start_date, not test_start_time - check actual column name
        required_fields = ["test_id", "model_a_id", "model_b_id", "test_status", "company_id", "brand_id"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df


class ModelMonitoringTransformer(BaseDataTransformer):
    """
    Transform model monitoring data.
    
    Transformations:
    - Clean and validate model monitoring data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize model monitoring transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ModelMonitoring if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform model monitoring data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with model monitoring data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["monitoring_id", "model_id", "monitoring_date", "monitoring_start_time", "monitoring_end_time"]
        df = self.clean_nulls(df, columns=required_fields)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows during validation. "
                        f"Check validation errors for details."
                    )
                
                df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated data")
        
        # Step 4: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df

