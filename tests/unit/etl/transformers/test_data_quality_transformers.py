"""
Unit Tests for Data Quality Domain Transformers
================================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType

from src.ownlens.processing.etl.transformers.data_quality.data_quality import (
    QualityMetricTransformer,
    QualityRuleTransformer,
    QualityCheckTransformer,
    QualityAlertTransformer,
    ValidationResultTransformer,
)


class TestQualityMetricTransformer:
    """Test QualityMetricTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = QualityMetricTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = QualityMetricTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["metric_id", "table_name", "metric_name", "metric_value", "threshold", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = QualityMetricTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["metric_id", "table_name", "metric_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.data_quality.data_quality.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = QualityMetricTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestQualityRuleTransformer:
    """Test QualityRuleTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = QualityRuleTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = QualityRuleTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["rule_id", "rule_name", "table_name", "rule_type", "rule_expression", "is_active"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = QualityRuleTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["rule_id", "rule_name", "table_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.data_quality.data_quality.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = QualityRuleTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestQualityCheckTransformer:
    """Test QualityCheckTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = QualityCheckTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = QualityCheckTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["check_id", "rule_id", "table_name", "check_status", "records_checked", "records_failed", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = QualityCheckTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["check_id", "rule_id", "table_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.data_quality.data_quality.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_status_filtering(self, mock_spark_session):
        """Test transformation with check status filtering."""
        transformer = QualityCheckTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["check_id", "rule_id", "check_status"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, check_status="failed")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = QualityCheckTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestQualityAlertTransformer:
    """Test QualityAlertTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = QualityAlertTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = QualityAlertTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["alert_id", "check_id", "rule_id", "alert_type", "severity", "message", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = QualityAlertTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["alert_id", "check_id", "rule_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.data_quality.data_quality.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_severity_filtering(self, mock_spark_session):
        """Test transformation with severity filtering."""
        transformer = QualityAlertTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["alert_id", "check_id", "severity"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, min_severity="high")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = QualityAlertTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestValidationResultTransformer:
    """Test ValidationResultTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ValidationResultTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ValidationResultTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["result_id", "validation_id", "table_name", "validation_status", "errors", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ValidationResultTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["result_id", "validation_id", "table_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.data_quality.data_quality.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_status_filtering(self, mock_spark_session):
        """Test transformation with validation status filtering."""
        transformer = ValidationResultTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["result_id", "validation_id", "validation_status"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validation_status="failed")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ValidationResultTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

