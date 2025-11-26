"""
Unit Tests for Audit Domain Transformers
=========================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.ownlens.processing.etl.transformers.audit.audit import (
    AuditLogTransformer,
    DataChangeTransformer,
    DataAccessTransformer,
    DataLineageTransformer,
    SecurityEventTransformer,
    ComplianceEventTransformer,
)


class TestAuditLogTransformer:
    """Test AuditLogTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = AuditLogTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"validate_with_model": False}
        transformer = AuditLogTransformer(mock_spark_session, config)
        
        assert transformer.validate_with_model is False

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = AuditLogTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["log_id", "user_id", "action", "resource_type", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = AuditLogTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["log_id", "user_id", "action"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_aggregation(self, mock_spark_session):
        """Test transformation with aggregation."""
        transformer = AuditLogTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "action", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.groupBy = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, aggregate=True)
        
        assert result is not None
        assert mock_df.groupBy.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = AuditLogTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDataChangeTransformer:
    """Test DataChangeTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DataChangeTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DataChangeTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["change_id", "table_name", "record_id", "change_type", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = DataChangeTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["change_id", "table_name", "record_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DataChangeTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDataAccessTransformer:
    """Test DataAccessTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DataAccessTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DataAccessTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 800
        mock_df.columns = ["access_id", "user_id", "resource_type", "resource_id", "access_type", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = DataAccessTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 800
        mock_df.columns = ["access_id", "user_id", "resource_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DataAccessTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDataLineageTransformer:
    """Test DataLineageTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DataLineageTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DataLineageTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["lineage_id", "source_table", "target_table", "transformation_type", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = DataLineageTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 300
        mock_df.columns = ["lineage_id", "source_table", "target_table"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DataLineageTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestSecurityEventTransformer:
    """Test SecurityEventTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = SecurityEventTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = SecurityEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["event_id", "user_id", "event_type", "severity", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = SecurityEventTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["event_id", "user_id", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_severity_filtering(self, mock_spark_session):
        """Test transformation with severity filtering."""
        transformer = SecurityEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["event_id", "user_id", "severity"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, min_severity="high")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = SecurityEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestComplianceEventTransformer:
    """Test ComplianceEventTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ComplianceEventTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = ComplianceEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 150
        mock_df.columns = ["event_id", "compliance_type", "event_type", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = ComplianceEventTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 150
        mock_df.columns = ["event_id", "compliance_type", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.audit.audit.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_kafka_value_column(self, mock_spark_session):
        """Test transformation with Kafka value column."""
        transformer = ComplianceEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 150
        mock_df.columns = ["value", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert mock_df.withColumn.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ComplianceEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

