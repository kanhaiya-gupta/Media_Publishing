"""
Editorial Domain Transformers
==============================

All editorial-related transformers in one file.
Transform editorial data with Pydantic validation.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, avg, max as spark_max, when, lit

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator
from .....models.editorial.article import Article, ArticleCreate
from .....models.editorial.article_performance import ArticlePerformance
from .....models.editorial.article_content import ArticleContent
from .....models.editorial.author import Author
from .....models.editorial.author_performance import AuthorPerformance
from .....models.editorial.category_performance import CategoryPerformance
from .....models.editorial.headline_test import HeadlineTest
from .....models.editorial.trending_topic import TrendingTopic
from .....models.editorial.content_recommendation import ContentRecommendation
from .....models.editorial.content_version import ContentVersion
from .....models.editorial.media_asset import MediaAsset
from .....models.editorial.media_variant import MediaVariant
from .....models.editorial.content_media import ContentMedia
from .....models.editorial.media_collection import MediaCollection
from .....models.editorial.media_collection_item import MediaCollectionItem
from .....models.editorial.media_usage import MediaUsage
from .....models.editorial.content_event import ContentEvent

import logging

logger = logging.getLogger(__name__)


class ArticleTransformer(BaseDataTransformer):
    """
    Transform editorial article data.
    
    Transformations:
    - Clean and validate article data
    - Enrich with author/category information
    - Calculate article metrics
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize article transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Article if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform article data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with article data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        # Note: company_id and brand_id are required in BaseEntityWithCompanyBrand
        required_fields = ["article_id", "title", "content_url", "company_id", "brand_id"]
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


class AuthorPerformanceTransformer(BaseDataTransformer):
    """
    Transform author performance data.
    
    Transformations:
    - Clean and validate author performance data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize author performance transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = AuthorPerformance if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform author performance data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with author performance data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["performance_id", "author_id", "brand_id", "performance_date"]
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


class CategoryPerformanceTransformer(BaseDataTransformer):
    """
    Transform category performance data.
    
    Transformations:
    - Clean and validate category performance data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize category performance transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = CategoryPerformance if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform category performance data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with category performance data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["performance_id", "category_id", "brand_id", "performance_date"]
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


class ArticleContentTransformer(BaseDataTransformer):
    """
    Transform article content data.
    
    Transformations:
    - Clean and validate article content data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize article content transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ArticleContent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform article content data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with article content data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["content_id", "article_id", "brand_id", "content_version", "content_format", "content_body"]
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


class PerformanceTransformer(BaseDataTransformer):
    """
    Transform editorial performance data.
    
    Transformations:
    - Aggregate performance metrics
    - Calculate engagement rates
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize performance transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ArticlePerformance if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform performance data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Calculate derived metrics
        
        Args:
            df: Input DataFrame with performance data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["article_id", "brand_id", "performance_date"]
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
        
        # Step 4: Calculate derived metrics
        df = self._calculate_engagement_metrics(df)
        
        # Step 5: Add processing timestamp
        df = self.add_timestamp(df)
        
        return df
    
    def _calculate_engagement_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate engagement metrics."""
        # Calculate total_engagement from individual engagement metrics
        # total_engagement = total_likes + total_shares + total_comments + total_bookmarks
        df = df.withColumn(
            "total_engagement",
            (col("total_likes").cast("double") + 
             col("total_shares").cast("double") + 
             col("total_comments").cast("double") + 
             col("total_bookmarks").cast("double"))
        )
        
        # Calculate engagement rate
        df = df.withColumn(
            "engagement_rate",
            when(col("total_views") > 0, col("total_engagement") / col("total_views"))
            .otherwise(lit(0.0))
        )
        
        # Calculate completion rate from unique_readers (if available)
        # completion_rate = unique_readers / total_views (as a proxy for completion)
        # If completion_rate already exists in the data, use it; otherwise calculate from unique_readers
        if "completion_rate" not in df.columns:
            df = df.withColumn(
                "completion_rate",
                when(col("total_views") > 0, col("unique_readers").cast("double") / col("total_views"))
                .otherwise(lit(0.0))
            )
        
        return df


class AuthorTransformer(BaseDataTransformer):
    """
    Transform author data.
    
    Transformations:
    - Clean and validate author data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize author transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = Author if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform author data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with author data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["author_id", "brand_id", "author_name", "author_code"]
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


class HeadlineTestTransformer(BaseDataTransformer):
    """
    Transform headline test data.
    
    Transformations:
    - Clean and validate headline test data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize headline test transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = HeadlineTest if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform headline test data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with headline test data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["test_id", "article_id", "brand_id", "headline_variant_a", "test_start_time"]
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


class TrendingTopicTransformer(BaseDataTransformer):
    """
    Transform trending topic data.
    
    Transformations:
    - Clean and validate trending topic data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize trending topic transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = TrendingTopic if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform trending topic data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with trending topic data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["topic_id", "brand_id", "topic_name", "trending_score", "period_start", "period_end"]
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


class ContentRecommendationTransformer(BaseDataTransformer):
    """
    Transform content recommendation data.
    
    Transformations:
    - Clean and validate content recommendation data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content recommendation transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentRecommendation if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content recommendation data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content recommendation data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["recommendation_id", "brand_id", "recommendation_type", "recommendation_score"]
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


class ContentVersionTransformer(BaseDataTransformer):
    """
    Transform content version data.
    
    Transformations:
    - Clean and validate content version data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content version transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentVersion if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content version data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content version data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["version_id", "article_id", "content_id", "brand_id", "version_number", "version_type"]
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


class MediaAssetTransformer(BaseDataTransformer):
    """
    Transform media asset data.
    
    Transformations:
    - Clean and validate media asset data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize media asset transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = MediaAsset if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform media asset data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with media asset data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["media_id", "brand_id", "media_type", "storage_path"]
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


class MediaVariantTransformer(BaseDataTransformer):
    """
    Transform media variant data.
    
    Transformations:
    - Clean and validate media variant data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize media variant transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = MediaVariant if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform media variant data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with media variant data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["variant_id", "media_id", "variant_type"]
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


class ContentMediaTransformer(BaseDataTransformer):
    """
    Transform content media data.
    
    Transformations:
    - Clean and validate content media data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize content media transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentMedia if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform content media data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with content media data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["relationship_id", "article_id", "media_id"]
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


class MediaCollectionTransformer(BaseDataTransformer):
    """
    Transform media collection data.
    
    Transformations:
    - Clean and validate media collection data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize media collection transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = MediaCollection if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform media collection data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with media collection data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["collection_id", "brand_id", "collection_name"]
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


class MediaCollectionItemTransformer(BaseDataTransformer):
    """
    Transform media collection item data.
    
    Transformations:
    - Clean and validate media collection item data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize media collection item transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = MediaCollectionItem if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform media collection item data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with media collection item data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["item_id", "collection_id", "media_id"]
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


class MediaUsageTransformer(BaseDataTransformer):
    """
    Transform media usage data.
    
    Transformations:
    - Clean and validate media usage data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize media usage transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = MediaUsage if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform media usage data.
        
        Steps:
        1. Clean nulls in required fields
        2. Transform column names/types to match schema
        3. Validate against Pydantic model (if enabled)
        4. Add processing timestamp
        
        Args:
            df: Input DataFrame with media usage data
            **kwargs: Transform parameters
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Step 1: Clean nulls in required fields
        required_fields = ["usage_id", "media_id", "usage_date"]
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


class EditorialContentEventTransformer(BaseDataTransformer):
    """
    Transform editorial content event data.
    
    Transformations:
    - Clean and validate content event data
    - Validate against Pydantic models
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize editorial content event transformer."""
        super().__init__(spark, config)
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = ContentEvent if self.validate_with_model else None
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform editorial content event data.
        
        Steps:
        0. Parse Kafka value column if present (for Kafka data)
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
        # Step 0: Parse Kafka value column if present (for Kafka data)
        if "value" in df.columns:
            from pyspark.sql.functions import from_json, col, to_date, when
            from pyspark.sql.types import StringType, StructType, StructField, MapType
            
            logger.info("Detected Kafka data format - parsing 'value' column from JSON")
            
            df = df.withColumn("value_str", col("value").cast(StringType()))
            
            json_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("article_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_timestamp", StringType(), True),
                StructField("event_date", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("author_id", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("city_id", StringType(), True),
                StructField("device_type_id", StringType(), True),
                StructField("os_id", StringType(), True),
                StructField("browser_id", StringType(), True),
                StructField("referrer", StringType(), True),
                StructField("referrer_type", StringType(), True),
                StructField("engagement_metrics", MapType(StringType(), StringType()), True),
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
        required_fields = ["event_id", "article_id", "brand_id", "event_type", "event_timestamp"]
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
