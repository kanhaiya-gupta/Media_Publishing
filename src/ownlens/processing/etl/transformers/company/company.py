"""
Company Domain Transformers
===========================

All company-related transformers in one file.
Transform company data with Pydantic validation.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, when, lit

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator
from .....models.company.employee_engagement import EmployeeEngagement
from .....models.company.department import Department
from .....models.company.employee import Employee
from .....models.company.communications_analytics import CommunicationsAnalytics
from .....models.company.content_performance import ContentPerformance
from .....models.company.department_performance import DepartmentPerformance
from .....models.company.content_event import ContentEvent
from .....models.company.internal_content import InternalContent

import logging

logger = logging.getLogger(__name__)


class EngagementTransformer(BaseDataTransformer):
    """
    Transform company employee engagement data.
    
    Transformations:
    - Aggregate engagement metrics
    - Calculate engagement scores
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize engagement transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = EmployeeEngagement if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform engagement data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Calculate engagement metrics
        5. Add processing timestamp
        
        Args:
            df: Input DataFrame with engagement data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["engagement_id", "employee_id", "company_id", "brand_id", "engagement_date"]
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
        
        # Step 4: Calculate engagement metrics
        df = self._calculate_engagement_scores(df)
        
        # Step 5: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df
    
    def _calculate_engagement_scores(self, df: DataFrame) -> DataFrame:
        """Calculate engagement scores."""
        # Calculate average engagement score
        # Use content_views instead of total_content_views (which doesn't exist)
        df = df.withColumn(
            "avg_engagement_score",
            when(col("content_views") > 0, col("content_interactions") / col("content_views"))
            .otherwise(lit(0.0))
        )
        
        return df


class DepartmentTransformer(BaseDataTransformer):
    """
    Transform department data.
    
    Transformations:
    - Clean and validate department data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize department transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Department if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform department data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with department data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["department_id", "company_id", "department_name", "department_code"]
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


class ContentEventTransformer(BaseDataTransformer):
    """
    Transform company content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content event data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "content_id", "company_id", "event_type", "event_timestamp"]
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


class InternalContentTransformer(BaseDataTransformer):
    """
    Transform company internal content data.
    
    Transformations:
    - Clean and validate internal content data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize internal content transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = InternalContent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform internal content data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with internal content data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        required_fields = ["content_id", "title", "content_type", "publish_time", "company_id", "brand_id"]
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


class EmployeeTransformer(BaseDataTransformer):
    """
    Transform employee data.
    
    Transformations:
    - Clean and validate employee data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize employee transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Employee if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform employee data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with employee data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: department_id is Optional in the Employee model, so don't filter on it
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        required_fields = ["employee_id", "company_id", "user_id", "brand_id"]
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


class ContentEventTransformer(BaseDataTransformer):
    """
    Transform company content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content event data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "content_id", "company_id", "event_type", "event_timestamp"]
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


class CommunicationsAnalyticsTransformer(BaseDataTransformer):
    """
    Transform communications analytics data.
    
    Transformations:
    - Clean and validate communications analytics data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize communications analytics transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = CommunicationsAnalytics if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform communications analytics data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with communications analytics data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["analytics_id", "company_id", "brand_id", "analytics_date"]
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


class ContentEventTransformer(BaseDataTransformer):
    """
    Transform company content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content event data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "content_id", "company_id", "event_type", "event_timestamp"]
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


class ContentPerformanceTransformer(BaseDataTransformer):
    """
    Transform company content performance data.
    
    Transformations:
    - Clean and validate content performance data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content performance transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentPerformance if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content performance data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content performance data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["performance_id", "content_id", "company_id", "performance_date"]
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


class ContentEventTransformer(BaseDataTransformer):
    """
    Transform company content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content event data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "content_id", "company_id", "event_type", "event_timestamp"]
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


class DepartmentPerformanceTransformer(BaseDataTransformer):
    """
    Transform company department performance data.
    
    Transformations:
    - Clean and validate department performance data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize department performance transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = DepartmentPerformance if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform department performance data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with department performance data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["performance_id", "department_id", "company_id", "performance_date"]
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


class ContentEventTransformer(BaseDataTransformer):
    """
    Transform company content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content event data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "content_id", "company_id", "event_type", "event_timestamp"]
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
