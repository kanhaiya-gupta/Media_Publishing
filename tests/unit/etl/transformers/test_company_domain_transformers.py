"""
Unit Tests for Company Domain Specific Transformers
===================================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

from src.ownlens.processing.etl.transformers.company.company import (
    EngagementTransformer,
    DepartmentTransformer,
    ContentEventTransformer,
    InternalContentTransformer,
    EmployeeTransformer,
    CommunicationsAnalyticsTransformer,
    ContentPerformanceTransformer,
    DepartmentPerformanceTransformer,
)


class TestEngagementTransformer:
    """Test EngagementTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = EngagementTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"aggregation_window_days": 30}
        transformer = EngagementTransformer(mock_spark_session, config)
        
        assert transformer.config == config

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = EngagementTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["engagement_id", "user_id", "content_id", "engagement_type", "engagement_date"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_aggregation(self, mock_spark_session):
        """Test transformation with aggregation."""
        transformer = EngagementTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["user_id", "content_id", "engagement_type", "engagement_date"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, aggregate=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = EngagementTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDepartmentTransformer:
    """Test DepartmentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DepartmentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DepartmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["department_id", "department_name", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with validation."""
        transformer = DepartmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["department_id", "department_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validate=True)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DepartmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestContentEventTransformer:
    """Test ContentEventTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ContentEventTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["event_id", "content_id", "event_type", "event_timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_kafka_value_column(self, mock_spark_session):
        """Test transformation with Kafka value column."""
        transformer = ContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["value", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert mock_df.withColumn.called

    def test_transform_with_event_type_filtering(self, mock_spark_session):
        """Test transformation with event type filtering."""
        transformer = ContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["event_id", "content_id", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, event_types=["view", "click"])
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ContentEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestInternalContentTransformer:
    """Test InternalContentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = InternalContentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = InternalContentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["content_id", "title", "content", "department_id", "author_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with validation."""
        transformer = InternalContentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["content_id", "title", "content"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validate=True)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = InternalContentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestEmployeeTransformer:
    """Test EmployeeTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = EmployeeTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = EmployeeTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["employee_id", "user_id", "department_id", "company_id", "brand_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with validation."""
        transformer = EmployeeTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["employee_id", "user_id", "department_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validate=True)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = EmployeeTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestCommunicationsAnalyticsTransformer:
    """Test CommunicationsAnalyticsTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = CommunicationsAnalyticsTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = CommunicationsAnalyticsTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["analytics_id", "communication_id", "metrics", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_aggregation(self, mock_spark_session):
        """Test transformation with aggregation."""
        transformer = CommunicationsAnalyticsTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["communication_id", "metrics", "period_start"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, aggregate=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = CommunicationsAnalyticsTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestContentPerformanceTransformer:
    """Test ContentPerformanceTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ContentPerformanceTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ContentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 400
        mock_df.columns = ["performance_id", "content_id", "views", "clicks", "shares", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_aggregation(self, mock_spark_session):
        """Test transformation with aggregation."""
        transformer = ContentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 400
        mock_df.columns = ["content_id", "views", "clicks", "period_start"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, aggregate=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ContentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDepartmentPerformanceTransformer:
    """Test DepartmentPerformanceTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DepartmentPerformanceTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DepartmentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["performance_id", "department_id", "metrics", "period_start", "period_end"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_aggregation(self, mock_spark_session):
        """Test transformation with aggregation."""
        transformer = DepartmentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["department_id", "metrics", "period_start"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, aggregate=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DepartmentPerformanceTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

