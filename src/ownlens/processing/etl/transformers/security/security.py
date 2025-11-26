"""
Security Domain Transformers
=============================

All security-related transformers in one file.
Transform security data with Pydantic validation.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator
from .....models.audit.security_event import SecurityEvent
from .....models.security.role import Role
from .....models.security.permission import Permission
from .....models.security.role_permission import RolePermission
from .....models.security.user_role import UserRole
from .....models.security.api_key import ApiKey
from .....models.security.api_key_usage import ApiKeyUsage
from .....models.security.user_session import UserSession

import logging

logger = logging.getLogger(__name__)


class SecurityEventTransformer(BaseDataTransformer):
    """
    Transform security event data.
    
    Transformations:
    - Clean and validate security event data
    - Aggregate security metrics
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize security event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = SecurityEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform security event data.
        
        Steps:
        0. Parse Kafka value column if present (for Kafka data)
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with security event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 0: Parse Kafka value column if present (for Kafka data)
        if "value" in df.columns:
            from pyspark.sql.functions import from_json, col, to_date, when
            from pyspark.sql.types import StringType, StructType, StructField, MapType
            
            logger.info("Detected Kafka data format - parsing 'value' column from JSON")
            
            df = df.withColumn("value_str", col("value").cast(StringType()))
            
            json_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_severity", StringType(), True),
                StructField("description", StringType(), True),
                StructField("success", StringType(), True),
                StructField("failure_reason", StringType(), True),
                StructField("event_timestamp", StringType(), True),
                StructField("event_date", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("event_data", MapType(StringType(), StringType()), True),
            ])
            
            df = df.withColumn("parsed_data", from_json(col("value_str"), json_schema))
            df = df.select(
                col("parsed_data.*"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            df = df.drop("value", "value_str")
            
            # Ensure event_date is set - derive from event_timestamp if not present
            if "event_timestamp" in df.columns and "event_date" in df.columns:
                df = df.withColumn(
                    "event_date",
                    when(
                        (col("event_date").isNull()) | (col("event_date") == ""),
                        to_date(col("event_timestamp"))
                    ).otherwise(to_date(col("event_date")))
                )
            elif "event_timestamp" in df.columns:
                df = df.withColumn("event_date", to_date(col("event_timestamp")))
        
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "event_type", "event_severity", "success", "event_timestamp"]
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


class UserSessionTransformer(BaseDataTransformer):
    """
    Transform security user session data.
    
    Transformations:
    - Clean and validate user session data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize user session transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = UserSession if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform user session data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with user session data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["session_id", "user_id", "session_token", "expires_at"]
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
    
    def aggregate_security_events(self, df: DataFrame, group_by: list) -> DataFrame:
        """
        Aggregate security event data by specified columns.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            
        Returns:
            Aggregated DataFrame
        """
        agg_exprs = [
            count("event_id").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            sum(spark_sum("success").cast("int")).alias("success_count"),
            (count("event_id") - sum(spark_sum("success").cast("int"))).alias("failure_count"),
        ]
        
        return df.groupBy(*group_by).agg(*agg_exprs)


class RoleTransformer(BaseDataTransformer):
    """
    Transform role data.
    
    Transformations:
    - Clean and validate role data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize role transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Role if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform role data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with role data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["role_id", "role_name", "role_code"]
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


class PermissionTransformer(BaseDataTransformer):
    """
    Transform permission data.
    
    Transformations:
    - Clean and validate permission data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize permission transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Permission if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform permission data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with permission data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["permission_id", "permission_name", "permission_code"]
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


class RolePermissionTransformer(BaseDataTransformer):
    """
    Transform role permission data.
    
    Transformations:
    - Clean and validate role permission data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize role permission transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = RolePermission if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform role permission data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with role permission data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["role_permission_id", "role_id", "permission_id"]
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


class UserRoleTransformer(BaseDataTransformer):
    """
    Transform user role data.
    
    Transformations:
    - Clean and validate user role data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize user role transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = UserRole if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform user role data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with user role data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        required_fields = ["user_role_id", "user_id", "role_id", "company_id", "brand_id"]
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


class APIKeyTransformer(BaseDataTransformer):
    """
    Transform API key data.
    
    Transformations:
    - Clean and validate API key data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize API key transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ApiKey if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform API key data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with API key data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["api_key_id", "company_id", "brand_id", "api_key_hash", "api_key_name"]
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


class APIKeyUsageTransformer(BaseDataTransformer):
    """
    Transform API key usage data.
    
    Transformations:
    - Clean and validate API key usage data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize API key usage transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ApiKeyUsage if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform API key usage data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with API key usage data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["usage_id", "api_key_id", "usage_date"]
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
