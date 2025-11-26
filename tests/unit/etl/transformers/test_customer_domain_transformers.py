"""
Unit Tests for Customer Domain Specific Transformers
====================================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

from src.ownlens.processing.etl.transformers.customer.customer import (
    SessionTransformer,
    UserEventTransformer,
    FeatureEngineeringTransformer,
    UserSegmentTransformer,
    UserSegmentAssignmentTransformer,
    ChurnPredictionTransformer,
    RecommendationTransformer,
    ConversionPredictionTransformer,
)


class TestSessionTransformer:
    """Test SessionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = SessionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"validate_with_model": False}
        transformer = SessionTransformer(mock_spark_session, config)
        
        assert transformer.validate_with_model is False

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = SessionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["session_id", "user_id", "session_start", "session_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_kafka_value_column(self, mock_spark_session):
        """Test transformation with Kafka value column."""
        transformer = SessionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["value", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert mock_df.withColumn.called

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = SessionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["session_id", "user_id", "session_start"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_without_validation(self, mock_spark_session):
        """Test transformation without Pydantic validation."""
        transformer = SessionTransformer(mock_spark_session, {"validate_with_model": False})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["session_id", "user_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validate=False)
        
        assert result is not None

    def test_transform_with_empty_dataframe(self, mock_spark_session):
        """Test transformation with empty DataFrame."""
        transformer = SessionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        mock_df.columns = []
        mock_df.withColumn = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = SessionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestUserEventTransformer:
    """Test UserEventTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = UserEventTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"validate_with_model": False}
        transformer = UserEventTransformer(mock_spark_session, config)
        
        assert transformer.validate_with_model is False

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = UserEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["event_id", "user_id", "event_type", "event_timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_kafka_value_column(self, mock_spark_session):
        """Test transformation with Kafka value column."""
        transformer = UserEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["value", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert mock_df.withColumn.called

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = UserEventTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["event_id", "user_id", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_event_type_filtering(self, mock_spark_session):
        """Test transformation with event type filtering."""
        transformer = UserEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["event_id", "user_id", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, event_types=["page_view", "click"])
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = UserEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestFeatureEngineeringTransformer:
    """Test FeatureEngineeringTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = FeatureEngineeringTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"feature_window_days": 30}
        transformer = FeatureEngineeringTransformer(mock_spark_session, config)
        
        assert transformer.config == config

    def test_transform_success(self, mock_spark_session):
        """Test successful feature engineering."""
        transformer = FeatureEngineeringTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "event_type", "event_timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_aggregation_features(self, mock_spark_session):
        """Test transformation with aggregation features."""
        transformer = FeatureEngineeringTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "event_type", "event_timestamp", "value"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, include_aggregations=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_with_time_window_features(self, mock_spark_session):
        """Test transformation with time window features."""
        transformer = FeatureEngineeringTransformer(mock_spark_session, {"feature_window_days": 7})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "event_timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, time_window_days=7)
        
        assert result is not None

    def test_transform_with_rolling_features(self, mock_spark_session):
        """Test transformation with rolling window features."""
        transformer = FeatureEngineeringTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "event_timestamp", "value"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, include_rolling_features=True)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = FeatureEngineeringTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestUserSegmentTransformer:
    """Test UserSegmentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = UserSegmentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = UserSegmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["segment_id", "segment_name", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = UserSegmentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["segment_id", "segment_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = UserSegmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestUserSegmentAssignmentTransformer:
    """Test UserSegmentAssignmentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = UserSegmentAssignmentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = UserSegmentAssignmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "segment_id", "assigned_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = UserSegmentAssignmentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "segment_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_deduplication(self, mock_spark_session):
        """Test transformation with deduplication."""
        transformer = UserSegmentAssignmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "segment_id", "assigned_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.dropDuplicates = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, deduplicate=True)
        
        assert result is not None
        assert mock_df.dropDuplicates.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = UserSegmentAssignmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestChurnPredictionTransformer:
    """Test ChurnPredictionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ChurnPredictionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ChurnPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["prediction_id", "user_id", "churn_probability", "prediction_date"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ChurnPredictionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["prediction_id", "user_id", "churn_probability"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_probability_threshold(self, mock_spark_session):
        """Test transformation with probability threshold."""
        transformer = ChurnPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["user_id", "churn_probability"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, probability_threshold=0.5)
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ChurnPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestRecommendationTransformer:
    """Test RecommendationTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = RecommendationTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = RecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["recommendation_id", "user_id", "article_id", "score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = RecommendationTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["recommendation_id", "user_id", "article_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_score_filtering(self, mock_spark_session):
        """Test transformation with score filtering."""
        transformer = RecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "article_id", "score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, min_score=0.5)
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_with_top_n(self, mock_spark_session):
        """Test transformation with top N recommendations."""
        transformer = RecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "article_id", "score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.orderBy = Mock(return_value=mock_df)
        mock_df.limit = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, top_n=10)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = RecommendationTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestConversionPredictionTransformer:
    """Test ConversionPredictionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ConversionPredictionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ConversionPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["prediction_id", "user_id", "conversion_probability", "prediction_date"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ConversionPredictionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["prediction_id", "user_id", "conversion_probability"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.customer.customer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_probability_threshold(self, mock_spark_session):
        """Test transformation with probability threshold."""
        transformer = ConversionPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["user_id", "conversion_probability"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, probability_threshold=0.7)
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ConversionPredictionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

