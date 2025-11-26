"""
Unit Tests for Compliance Domain Transformers
==============================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

from src.ownlens.processing.etl.transformers.compliance.compliance import (
    ComplianceEventTransformer,
    UserConsentTransformer,
    DataSubjectRequestTransformer,
    RetentionPolicyTransformer,
    RetentionExecutionTransformer,
    AnonymizedDataTransformer,
    PrivacyAssessmentTransformer,
    BreachIncidentTransformer,
)


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
        mock_df.count.return_value = 200
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
        mock_df.count.return_value = 200
        mock_df.columns = ["event_id", "compliance_type", "event_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = ComplianceEventTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestUserConsentTransformer:
    """Test UserConsentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = UserConsentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = UserConsentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["consent_id", "user_id", "consent_type", "consent_status", "timestamp"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = UserConsentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["consent_id", "user_id", "consent_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_consent_status_filtering(self, mock_spark_session):
        """Test transformation with consent status filtering."""
        transformer = UserConsentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "consent_type", "consent_status"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, consent_status="granted")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = UserConsentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestDataSubjectRequestTransformer:
    """Test DataSubjectRequestTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = DataSubjectRequestTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = DataSubjectRequestTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["request_id", "user_id", "request_type", "status", "created_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = DataSubjectRequestTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["request_id", "user_id", "request_type"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = DataSubjectRequestTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestRetentionPolicyTransformer:
    """Test RetentionPolicyTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = RetentionPolicyTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = RetentionPolicyTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["policy_id", "table_name", "retention_days", "is_active"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = RetentionPolicyTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["policy_id", "table_name", "retention_days"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = RetentionPolicyTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestRetentionExecutionTransformer:
    """Test RetentionExecutionTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = RetentionExecutionTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = RetentionExecutionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["execution_id", "policy_id", "execution_status", "records_deleted", "executed_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = RetentionExecutionTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.columns = ["execution_id", "policy_id", "execution_status"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = RetentionExecutionTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestAnonymizedDataTransformer:
    """Test AnonymizedDataTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = AnonymizedDataTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = AnonymizedDataTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["anonymized_id", "original_table", "anonymized_table", "anonymization_method", "anonymized_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = AnonymizedDataTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_df.columns = ["anonymized_id", "original_table", "anonymized_table"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = AnonymizedDataTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestPrivacyAssessmentTransformer:
    """Test PrivacyAssessmentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = PrivacyAssessmentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = PrivacyAssessmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["assessment_id", "table_name", "assessment_score", "risk_level", "assessed_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = PrivacyAssessmentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["assessment_id", "table_name", "assessment_score"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = PrivacyAssessmentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)


class TestBreachIncidentTransformer:
    """Test BreachIncidentTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = BreachIncidentTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.validate_with_model is True

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = BreachIncidentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["incident_id", "incident_type", "severity", "affected_records", "detected_at", "resolved_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_validation(self, mock_spark_session):
        """Test transformation with Pydantic validation."""
        transformer = BreachIncidentTransformer(mock_spark_session, {"validate_with_model": True})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["incident_id", "incident_type", "severity"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.compliance.compliance.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df, validate=True)
            
            assert result is not None

    def test_transform_with_severity_filtering(self, mock_spark_session):
        """Test transformation with severity filtering."""
        transformer = BreachIncidentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["incident_id", "incident_type", "severity"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, min_severity="high")
        
        assert result is not None
        assert mock_df.filter.called

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = BreachIncidentTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

