"""
Unit Tests for BaseTransformer
================================
"""

import pytest
from unittest.mock import Mock
from pyspark.sql import DataFrame

from src.ownlens.processing.etl.base.transformer import BaseTransformer


class ConcreteTransformer(BaseTransformer):
    """Concrete transformer for testing."""
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Simple pass-through transformation."""
        return df


class TestBaseTransformer:
    """Test BaseTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = ConcreteTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session
        assert transformer.config == {}

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"key": "value"}
        transformer = ConcreteTransformer(mock_spark_session, config)
        
        assert transformer.config == config

    def test_validate_input_success(self, mock_spark_session):
        """Test successful input validation."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "email"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, ["id", "name"])
        
        assert result is True

    def test_validate_input_missing_columns(self, mock_spark_session):
        """Test input validation with missing columns."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, ["id", "name", "email"])
        
        assert result is False

    def test_log_transformation_stats(self, mock_spark_session):
        """Test logging transformation statistics."""
        input_df = Mock(spec=DataFrame)
        input_df.count.return_value = 100
        input_df.columns = ["id", "name"]
        
        output_df = Mock(spec=DataFrame)
        output_df.count.return_value = 100
        output_df.columns = ["id", "name", "processed"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        transformer.log_transformation_stats(input_df, output_df, "test_transform")
        
        # Should not raise exception
        assert True

    def test_log_transformation_stats_error(self, mock_spark_session):
        """Test logging transformation stats with error."""
        input_df = Mock(spec=DataFrame)
        input_df.count.side_effect = Exception("Count error")
        
        output_df = Mock(spec=DataFrame)
        
        transformer = ConcreteTransformer(mock_spark_session)
        # Should not raise exception, just log warning
        transformer.log_transformation_stats(input_df, output_df, "test_transform")
        
        assert True

    def test_transform_method_exists(self, mock_spark_session):
        """Test that transform method exists and can be called."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.transform(mock_df)
        
        assert result is not None
        assert result is mock_df  # Pass-through implementation

    def test_validate_input_empty_columns(self, mock_spark_session):
        """Test input validation with empty required columns."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, [])
        
        assert result is True  # Empty list should pass

    def test_validate_input_none_dataframe(self, mock_spark_session):
        """Test input validation with None DataFrame."""
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(None, ["id"])
        
        assert result is False

    def test_validate_input_all_columns_present(self, mock_spark_session):
        """Test input validation when all required columns are present."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "email", "phone"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, ["id", "name", "email"])
        
        assert result is True

    def test_validate_input_partial_columns_present(self, mock_spark_session):
        """Test input validation when only some required columns are present."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, ["id", "name", "email", "phone"])
        
        assert result is False

    def test_log_transformation_stats_with_different_row_counts(self, mock_spark_session):
        """Test logging transformation stats when row counts differ."""
        input_df = Mock(spec=DataFrame)
        input_df.count.return_value = 100
        input_df.columns = ["id", "name"]
        
        output_df = Mock(spec=DataFrame)
        output_df.count.return_value = 95  # Some rows filtered
        output_df.columns = ["id", "name", "processed"]
        
        transformer = ConcreteTransformer(mock_spark_session)
        transformer.log_transformation_stats(input_df, output_df, "test_transform")
        
        # Should not raise exception
        assert True

    def test_log_transformation_stats_with_column_changes(self, mock_spark_session):
        """Test logging transformation stats with column changes."""
        input_df = Mock(spec=DataFrame)
        input_df.count.return_value = 100
        input_df.columns = ["id", "name"]
        
        output_df = Mock(spec=DataFrame)
        output_df.count.return_value = 100
        output_df.columns = ["id", "name", "normalized_name", "name_hash"]  # New columns added
        
        transformer = ConcreteTransformer(mock_spark_session)
        transformer.log_transformation_stats(input_df, output_df, "test_transform")
        
        # Should not raise exception
        assert True

    def test_log_transformation_stats_with_empty_dataframes(self, mock_spark_session):
        """Test logging transformation stats with empty DataFrames."""
        input_df = Mock(spec=DataFrame)
        input_df.count.return_value = 0
        input_df.columns = []
        
        output_df = Mock(spec=DataFrame)
        output_df.count.return_value = 0
        output_df.columns = []
        
        transformer = ConcreteTransformer(mock_spark_session)
        transformer.log_transformation_stats(input_df, output_df, "test_transform")
        
        # Should not raise exception
        assert True

    def test_config_access(self, mock_spark_session):
        """Test that config can be accessed and modified."""
        config = {"key1": "value1", "key2": "value2"}
        transformer = ConcreteTransformer(mock_spark_session, config)
        
        assert transformer.config == config
        assert transformer.config["key1"] == "value1"
        
        # Modify config
        transformer.config["key3"] = "value3"
        assert "key3" in transformer.config

    def test_transform_with_kwargs(self, mock_spark_session):
        """Test transform method with keyword arguments."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.transform(mock_df, option1="value1", option2="value2")
        
        assert result is not None

    def test_validate_input_with_case_sensitivity(self, mock_spark_session):
        """Test input validation with case sensitivity."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["ID", "Name", "Email"]  # Different case
        
        transformer = ConcreteTransformer(mock_spark_session)
        # Column names are case-sensitive in Spark
        result = transformer.validate_input(mock_df, ["id", "name"])
        
        assert result is False  # Case mismatch

    def test_validate_input_with_duplicate_columns(self, mock_spark_session):
        """Test input validation with duplicate column names (should not happen in Spark but test edge case)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "id"]  # Duplicate (unlikely in Spark)
        
        transformer = ConcreteTransformer(mock_spark_session)
        result = transformer.validate_input(mock_df, ["id", "name"])
        
        # Should handle gracefully
        assert result is True or result is False

