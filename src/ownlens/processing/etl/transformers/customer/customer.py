"""
Customer Domain Transformers
============================

All customer-related transformers in one file.
Transform customer data with Pydantic validation.
"""

from typing import Optional, Dict, Any, List, Type
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct, avg, max as spark_max, min as spark_min, datediff, current_date

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator
from .....models.customer.session import Session, SessionCreate
from .....models.customer.user_event import UserEvent, UserEventCreate
from .....models.customer.user_segment import UserSegment, UserSegmentAssignment
from .....models.customer.churn_prediction import ChurnPrediction
from .....models.customer.recommendation import Recommendation
from .....models.customer.conversion_prediction import ConversionPrediction
from pydantic import BaseModel

import logging

logger = logging.getLogger(__name__)


class SessionTransformer(BaseDataTransformer):
    """
    Transform customer session data.
    
    Transformations:
    - Calculate session metrics
    - Aggregate session data
    - Enrich with user information
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize session transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Session if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform session data.
        
        Steps:
        0. Parse Kafka value column if present (for Kafka data)
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with session data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 0: Parse Kafka value column if present (for Kafka data)
        if "value" in df.columns:
            from pyspark.sql.functions import from_json, col
            from pyspark.sql.types import StringType, StructType, StructField, MapType, ArrayType
            
            logger.info("Detected Kafka data format - parsing 'value' column from JSON")
            
            df = df.withColumn("value_str", col("value").cast(StringType()))
            
            json_schema = StructType([
                StructField("session_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("city_id", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("device_type_id", StringType(), True),
                StructField("os_id", StringType(), True),
                StructField("browser_id", StringType(), True),
                StructField("referrer", StringType(), True),
                StructField("user_segment", StringType(), True),
                StructField("subscription_tier", StringType(), True),
                StructField("session_start", StringType(), True),
                StructField("session_end", StringType(), True),
                StructField("session_duration_sec", StringType(), True),
                StructField("total_events", StringType(), True),
                StructField("article_views", StringType(), True),
                StructField("article_clicks", StringType(), True),
                StructField("video_plays", StringType(), True),
                StructField("newsletter_signups", StringType(), True),
                StructField("ad_clicks", StringType(), True),
                StructField("searches", StringType(), True),
                StructField("pages_visited_count", StringType(), True),
                StructField("unique_pages_count", StringType(), True),
                StructField("unique_categories_count", StringType(), True),
                StructField("categories_visited", ArrayType(StringType()), True),
                StructField("article_ids_visited", ArrayType(StringType()), True),
                StructField("article_titles_visited", ArrayType(StringType()), True),
                StructField("page_events", ArrayType(MapType(StringType(), StringType())), True),
                StructField("engagement_score", StringType(), True),
                StructField("scroll_depth_avg", StringType(), True),
                StructField("time_on_page_avg", StringType(), True),
                StructField("batch_id", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
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
        
        # Step 1: Clean nulls in required fields
        required_fields = ["session_id", "user_id", "brand_id", "session_start"]
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
    
    def aggregate_sessions(self, df: DataFrame, group_by: list) -> DataFrame:
        """
        Aggregate session data by specified columns.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            
        Returns:
            Aggregated DataFrame
        """
        agg_exprs = [
            count("session_id").alias("session_count"),
            spark_sum("duration_seconds").alias("total_duration"),
            avg("duration_seconds").alias("avg_duration"),
            spark_max("duration_seconds").alias("max_duration"),
            spark_min("duration_seconds").alias("min_duration"),
        ]
        
        return df.groupBy(*group_by).agg(*agg_exprs)


class UserEventTransformer(BaseDataTransformer):
    """
    Transform customer user event data.
    
    Transformations:
    - Parse event data
    - Aggregate events
    - Calculate event metrics
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize user event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = UserEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform user event data.
        
        Steps:
        0. Parse Kafka value column if present (for Kafka data)
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with user event data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 0: Parse Kafka value column if present (for Kafka data)
        # Kafka messages have a 'value' column containing JSON that needs to be parsed
        if "value" in df.columns:
            from pyspark.sql.functions import from_json, col
            from pyspark.sql.types import StringType, StructType, StructField, MapType
            
            logger.info("Detected Kafka data format - parsing 'value' column from JSON")
            
            # Cast value to string (Kafka value is binary by default)
            df = df.withColumn("value_str", col("value").cast(StringType()))
            
            # Define a permissive schema with all fields as optional strings
            # The model validator will handle proper type conversion
            json_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_timestamp", StringType(), True),
                StructField("event_date", StringType(), True),
                StructField("article_id", StringType(), True),
                StructField("article_title", StringType(), True),
                StructField("article_type", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("city_id", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("device_type_id", StringType(), True),
                StructField("os_id", StringType(), True),
                StructField("browser_id", StringType(), True),
                StructField("engagement_metrics", MapType(StringType(), StringType()), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ])
            
            # Parse JSON
            df = df.withColumn("parsed_data", from_json(col("value_str"), json_schema))
            
            # Flatten the parsed data into columns
            df = df.select(
                col("parsed_data.*"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Drop temporary columns
            df = df.drop("value", "value_str")
            
            # Ensure event_date is set - derive from event_timestamp if not present
            # The model expects event_date to be a date, not a string
            from pyspark.sql.functions import to_date, col, when, isnull
            if "event_timestamp" in df.columns and "event_date" in df.columns:
                # If event_date is null or empty, derive it from event_timestamp
                df = df.withColumn(
                    "event_date",
                    when(
                        (col("event_date").isNull()) | (col("event_date") == ""),
                        to_date(col("event_timestamp"))
                    ).otherwise(to_date(col("event_date")))
                )
            elif "event_timestamp" in df.columns:
                # If event_date doesn't exist, create it from event_timestamp
                df = df.withColumn("event_date", to_date(col("event_timestamp")))
        
        # Step 1: Clean nulls in required fields
        required_fields = ["event_id", "user_id", "event_type", "event_timestamp"]
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
    
    def aggregate_events(self, df: DataFrame, group_by: list) -> DataFrame:
        """
        Aggregate event data by specified columns.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            
        Returns:
            Aggregated DataFrame
        """
        agg_exprs = [
            count("event_id").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
        ]
        
        return df.groupBy(*group_by).agg(*agg_exprs)


class FeatureEngineeringTransformer(BaseDataTransformer):
    """
    Feature engineering for ML models with optional Pydantic validation.
    
    Creates features for:
    - Churn prediction
    - User segmentation
    - Recommendation models
    
    Optionally validates output against Pydantic models.
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize feature engineering transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", False)  # Default False for features
        self.model_class = self.config.get("model_class", None)  # Optional model class
    
    def transform(self, df: DataFrame, feature_type: str = "churn", **kwargs) -> DataFrame:
        """
        Create features for ML models.
        
        Args:
            df: Input DataFrame with user/event data
            feature_type: Type of features ("churn", "segmentation", "recommendation")
            **kwargs: Feature engineering parameters
            
        Returns:
            DataFrame with engineered features
        """
        # Create features
        if feature_type == "churn":
            result_df = self._create_churn_features(df, **kwargs)
        elif feature_type == "segmentation":
            result_df = self._create_segmentation_features(df, **kwargs)
        elif feature_type == "recommendation":
            result_df = self._create_recommendation_features(df, **kwargs)
        else:
            logger.warning(f"Unknown feature type: {feature_type}, returning original DataFrame")
            result_df = df
        
        # Optional: Validate against Pydantic model if provided
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    result_df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid feature rows during validation. "
                        f"Check validation errors for details."
                    )
                
                result_df = valid_df
                
            except Exception as e:
                logger.error(f"Error during Pydantic validation: {e}", exc_info=True)
                logger.warning("Continuing with unvalidated features")
        
        # Add processing timestamp
        result_df = self.add_timestamp(result_df)
        
        return result_df
    
    def _create_churn_features(self, df: DataFrame, **kwargs) -> DataFrame:
        """Create features for churn prediction."""
        # Example features:
        # - Days since last session
        # - Total sessions
        # - Average engagement score
        # - Subscription tier
        
        # This is a placeholder - actual implementation would depend on data schema
        return df
    
    def _create_segmentation_features(self, df: DataFrame, **kwargs) -> DataFrame:
        """Create features for user segmentation."""
        # Example features:
        # - User activity level
        # - Content preferences
        # - Engagement patterns
        
        return df
    
    def _create_recommendation_features(self, df: DataFrame, **kwargs) -> DataFrame:
        """Create features for recommendation models."""
        # Example features:
        # - User-item interactions
        # - Content similarity
        # - User preferences
        
        return df


class UserSegmentTransformer(BaseDataTransformer):
    """
    Transform user segment data.
    
    Transformations:
    - Clean and validate user segment data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize user segment transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = UserSegment if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform user segment data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with user segment data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["segment_id", "brand_id", "segment_name", "segment_code"]
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


class UserSegmentAssignmentTransformer(BaseDataTransformer):
    """
    Transform user segment assignment data.
    
    Transformations:
    - Clean and validate user segment assignment data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize user segment assignment transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = UserSegmentAssignment if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform user segment assignment data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with user segment assignment data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["assignment_id", "user_id", "segment_id", "assignment_date"]
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


class ChurnPredictionTransformer(BaseDataTransformer):
    """
    Transform churn prediction data.
    
    Transformations:
    - Clean and validate churn prediction data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize churn prediction transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ChurnPrediction if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform churn prediction data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with churn prediction data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["prediction_id", "user_id", "brand_id", "prediction_date", "churn_probability", "model_version"]
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


class RecommendationTransformer(BaseDataTransformer):
    """
    Transform recommendation data.
    
    Transformations:
    - Clean and validate recommendation data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize recommendation transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Recommendation if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform recommendation data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with recommendation data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["recommendation_id", "user_id", "brand_id", "article_id", "recommendation_score"]
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


class ConversionPredictionTransformer(BaseDataTransformer):
    """
    Transform conversion prediction data.
    
    Transformations:
    - Clean and validate conversion prediction data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize conversion prediction transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ConversionPrediction if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform conversion prediction data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with conversion prediction data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["prediction_id", "user_id", "brand_id", "prediction_date", "conversion_probability", "model_version"]
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

