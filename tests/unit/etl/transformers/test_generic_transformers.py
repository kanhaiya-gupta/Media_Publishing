"""
Unit Tests for Generic Transformers
====================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.ownlens.processing.etl.transformers.generic.pydantic_model_transformer import PydanticModelTransformer


class TestPydanticModelTransformer:
    """Test PydanticModelTransformer class."""

    def test_init(self, mock_spark_session):
        """Test transformer initialization."""
        transformer = PydanticModelTransformer(mock_spark_session)
        
        assert transformer.spark is mock_spark_session

    def test_init_with_config(self, mock_spark_session):
        """Test transformer initialization with config."""
        config = {"table_name": "companies", "model_module": "base.company"}
        transformer = PydanticModelTransformer(mock_spark_session, config)
        
        assert transformer.config == config

    def test_transform_success(self, mock_spark_session):
        """Test successful transformation."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "companies"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["company_id", "company_name", "company_code"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.generic.pydantic_model_transformer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df)
            
            assert result is not None
            assert isinstance(result, DataFrame) or result is mock_df

    def test_transform_with_table_name_mapping(self, mock_spark_session):
        """Test transformation with table name mapping."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "users"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["user_id", "email", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.generic.pydantic_model_transformer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df)
            
            assert result is not None

    def test_transform_with_custom_model_class(self, mock_spark_session):
        """Test transformation with custom model class."""
        from pydantic import BaseModel
        
        class CustomModel(BaseModel):
            id: str
            name: str
        
        transformer = PydanticModelTransformer(mock_spark_session, {"model_class": CustomModel})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.columns = ["id", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.generic.pydantic_model_transformer.ModelValidator') as mock_validator:
            mock_validator.transform_to_model_schema.return_value = mock_df
            mock_validator.validate_dataframe.return_value = (mock_df, Mock(spec=DataFrame))
            
            result = transformer.transform(mock_df)
            
            assert result is not None

    def test_transform_with_validation_disabled(self, mock_spark_session):
        """Test transformation with validation disabled."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "companies", "validate": False})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["company_id", "company_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df, validate=False)
        
        assert result is not None

    def test_transform_with_unknown_table_name(self, mock_spark_session):
        """Test transformation with unknown table name."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "unknown_table"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        # Should handle gracefully without validation
        result = transformer.transform(mock_df)
        
        assert result is not None

    def test_transform_with_empty_dataframe(self, mock_spark_session):
        """Test transformation with empty DataFrame."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "companies"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        mock_df.columns = []
        mock_df.withColumn = Mock(return_value=mock_df)
        
        result = transformer.transform(mock_df)
        
        assert result is not None

    def test_transform_error_handling(self, mock_spark_session):
        """Test transformation error handling."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "companies"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.side_effect = Exception("Error")
        
        with pytest.raises(Exception, match="Error"):
            transformer.transform(mock_df)

    def test_transform_with_model_import_error(self, mock_spark_session):
        """Test transformation when model import fails."""
        transformer = PydanticModelTransformer(mock_spark_session, {"table_name": "companies"})
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["company_id", "company_name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        with patch('src.ownlens.processing.etl.transformers.generic.pydantic_model_transformer.importlib.import_module') as mock_import:
            mock_import.side_effect = ImportError("Module not found")
            
            # Should handle gracefully
            result = transformer.transform(mock_df)
            
            assert result is not None

    def test_transform_with_multiple_table_names(self, mock_spark_session):
        """Test transformation with multiple table names."""
        transformer = PydanticModelTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        # Test with different table names
        for table_name in ["companies", "users", "articles"]:
            transformer.config = {"table_name": table_name}
            result = transformer.transform(mock_df)
            assert result is not None

