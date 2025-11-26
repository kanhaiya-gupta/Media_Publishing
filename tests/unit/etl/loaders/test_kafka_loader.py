"""
Unit Tests for KafkaLoader
===========================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame

from src.ownlens.processing.etl.loaders.kafka_loader import KafkaLoader
from src.ownlens.processing.etl.utils.config import ETLConfig


class TestKafkaLoader:
    """Test KafkaLoader class."""

    def test_init(self, mock_spark_session, test_config):
        """Test loader initialization."""
        loader = KafkaLoader(mock_spark_session, test_config)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is test_config
        assert loader.kafka_config is not None

    def test_init_without_config(self, mock_spark_session):
        """Test loader initialization without config."""
        loader = KafkaLoader(mock_spark_session)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is not None

    def test_load_batch_success(self, mock_spark_session, test_config):
        """Test successful batch loading."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["key", "value"]
        mock_df.selectExpr = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = KafkaLoader(mock_spark_session, test_config)
        result = loader.load(mock_df, "customer-user-events", streaming=False)
        
        assert result is True
        mock_df.write.format.assert_called_once_with("kafka")

    def test_load_streaming_success(self, mock_spark_session, test_config):
        """Test successful streaming loading."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.writeStream = Mock()
        mock_df.writeStream.format.return_value = mock_df.writeStream
        mock_df.writeStream.options.return_value = mock_df.writeStream
        mock_df.writeStream.option.return_value = mock_df.writeStream
        mock_query = Mock()
        mock_query.start.return_value = mock_query
        mock_query.awaitTermination = Mock()
        mock_df.writeStream.start.return_value = mock_query
        
        loader = KafkaLoader(mock_spark_session, test_config)
        result = loader.load(
            mock_df,
            "customer-user-events",
            streaming=True,
            checkpoint_location="/tmp/checkpoint"
        )
        
        assert result is True
        mock_df.writeStream.format.assert_called_once_with("kafka")

    def test_load_empty_dataframe(self, mock_spark_session, test_config):
        """Test loading empty DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        
        loader = KafkaLoader(mock_spark_session, test_config)
        result = loader.load(mock_df, "customer-user-events")
        
        assert result is False

    def test_load_error_handling(self, mock_spark_session, test_config):
        """Test error handling during load."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.selectExpr = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.save.side_effect = Exception("Kafka connection error")
        
        loader = KafkaLoader(mock_spark_session, test_config)
        result = loader.load(mock_df, "customer-user-events")
        
        assert result is False

