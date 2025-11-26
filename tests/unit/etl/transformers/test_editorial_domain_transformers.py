"""
Unit Tests for Editorial Domain Specific Transformers
=====================================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, ArrayType

from src.ownlens.processing.etl.transformers.editorial.editorial import (
    ArticleTransformer,
    AuthorPerformanceTransformer,
    CategoryPerformanceTransformer,
    ArticleContentTransformer,
    PerformanceTransformer,
    AuthorTransformer,
    HeadlineTestTransformer,
    TrendingTopicTransformer,
    ContentRecommendationTransformer,
    ContentVersionTransformer,
    MediaAssetTransformer,
    MediaVariantTransformer,
    ContentMediaTransformer,
    MediaCollectionTransformer,
    MediaCollectionItemTransformer,
    MediaUsageTransformer,
    EditorialContentEventTransformer,
)


class TestArticleTransformer:
    """Test ArticleTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ArticleTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"validate_with_model": False}
        transformer = ArticleTransformer(mock_spark_session, config)
        
        assert transformer.validate_with_model is False

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ArticleTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["article_id", "title", "content_url", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ArticleTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["article_id", "title", "content_url"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_required_fields(self, mock_spark_session):
        """Test transformation with required fields cleaning."""
        transformer = ArticleTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["article_id", "title", "content_url", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.dropna = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ArticleTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestAuthorPerformanceTransformer:
    """Test AuthorPerformanceTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = AuthorPerformanceTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = AuthorPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["author_id", "performance_metrics", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = AuthorPerformanceTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["author_id", "performance_metrics"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = AuthorPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestCategoryPerformanceTransformer:
    """Test CategoryPerformanceTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = CategoryPerformanceTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = CategoryPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 30
        mock_df.columns = ["category_id", "performance_metrics", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = CategoryPerformanceTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 30
        mock_df.columns = ["category_id", "performance_metrics"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = CategoryPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestArticleContentTransformer:
    """Test ArticleContentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ArticleContentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ArticleContentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["article_id", "content", "content_type", "version"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ArticleContentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["article_id", "content", "content_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ArticleContentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestPerformanceTransformer:
    """Test PerformanceTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = PerformanceTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = PerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["article_id", "views", "clicks", "shares", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = PerformanceTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["article_id", "views", "clicks"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = PerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestAuthorTransformer:
    """Test AuthorTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = AuthorTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = AuthorTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["author_id", "author_name", "email", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = AuthorTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["author_id", "author_name", "email"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = AuthorTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestHeadlineTestTransformer:
    """Test HeadlineTestTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = HeadlineTestTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = HeadlineTestTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["test_id", "article_id", "headline_variant", "test_results"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = HeadlineTestTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["test_id", "article_id", "headline_variant"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = HeadlineTestTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestTrendingTopicTransformer:
    """Test TrendingTopicTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = TrendingTopicTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = TrendingTopicTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["topic_id", "topic_name", "trend_score", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = TrendingTopicTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["topic_id", "topic_name", "trend_score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = TrendingTopicTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestContentRecommendationTransformer:
    """Test ContentRecommendationTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ContentRecommendationTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ContentRecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["recommendation_id", "user_id", "article_id", "score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ContentRecommendationTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["recommendation_id", "user_id", "article_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ContentRecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestContentVersionTransformer:
    """Test ContentVersionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ContentVersionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ContentVersionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["version_id", "article_id", "version_number", "content"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ContentVersionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["version_id", "article_id", "version_number"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ContentVersionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestMediaAssetTransformer:
    """Test MediaAssetTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = MediaAssetTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = MediaAssetTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["asset_id", "asset_type", "file_path", "file_size"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = MediaAssetTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["asset_id", "asset_type", "file_path"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = MediaAssetTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestMediaVariantTransformer:
    """Test MediaVariantTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = MediaVariantTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = MediaVariantTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 400
        mock_df.columns = ["variant_id", "asset_id", "variant_type", "file_path"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = MediaVariantTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 400
        mock_df.columns = ["variant_id", "asset_id", "variant_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = MediaVariantTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestContentMediaTransformer:
    """Test ContentMediaTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ContentMediaTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ContentMediaTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["content_id", "media_id", "media_type", "position"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ContentMediaTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["content_id", "media_id", "media_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ContentMediaTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestMediaCollectionTransformer:
    """Test MediaCollectionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = MediaCollectionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = MediaCollectionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["collection_id", "collection_name", "description"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = MediaCollectionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["collection_id", "collection_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = MediaCollectionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestMediaCollectionItemTransformer:
    """Test MediaCollectionItemTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = MediaCollectionItemTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = MediaCollectionItemTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["collection_id", "media_id", "position", "added_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = MediaCollectionItemTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["collection_id", "media_id", "position"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = MediaCollectionItemTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestMediaUsageTransformer:
    """Test MediaUsageTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = MediaUsageTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = MediaUsageTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["usage_id", "media_id", "content_id", "usage_type", "usage_date"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = MediaUsageTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["usage_id", "media_id", "content_id", "usage_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = MediaUsageTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestEditorialContentEventTransformer:
    """Test EditorialContentEventTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = EditorialContentEventTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = EditorialContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 2000
        mock_df.columns = ["event_id", "content_id", "event_type", "event_timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = EditorialContentEventTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 2000
        mock_df.columns = ["event_id", "content_id", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.editorial.editorial.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_kafka_value_column(self, mock_spark_session):
        """Test transformation with Kafka value column."""
        transformer = EditorialContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 2000
        mock_df.columns = ["value", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert mock_df.withColumn.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = EditorialContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

