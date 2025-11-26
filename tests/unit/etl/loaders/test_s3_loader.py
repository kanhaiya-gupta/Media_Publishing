"""
Unit Tests for S3Loader
=======================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from datetime import datetime

from src.ownlens.processing.etl.loaders.s3_loader import S3Loader
from src.ownlens.processing.etl.utils.config import ETLConfig


class TestS3Loader:
    """Test S3Loader class."""

    def test_init(self, mock_spark_session, test_config):
        """Test loader initialization."""
        loader = S3Loader(mock_spark_session, test_config)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is test_config
        assert loader.s3_config is not None

    def test_init_without_config(self, mock_spark_session):
        """Test loader initialization without config."""
        loader = S3Loader(mock_spark_session)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is not None

    def test_init_configures_spark(self, mock_spark_session, test_config):
        """Test that initialization configures Spark for S3."""
        loader = S3Loader(mock_spark_session, test_config)
        
        # Verify Spark conf was set
        assert mock_spark_session.conf.set.called

    def test_load_parquet_success(self, mock_spark_session, test_config):
        """Test successful Parquet loading."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.partitionBy.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet")
        
        assert result is True
        mock_df.write.format.assert_called_once_with("parquet")

    def test_load_delta_success(self, mock_spark_session, test_config):
        """Test successful Delta Lake loading."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.partitionBy.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="delta")
        
        assert result is True
        mock_df.write.format.assert_called_once_with("delta")

    def test_load_json_success(self, mock_spark_session, test_config):
        """Test successful JSON loading."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="json")
        
        assert result is True
        mock_df.write.format.assert_called_once_with("json")

    def test_load_with_partitioning(self, mock_spark_session, test_config):
        """Test loading with partitioning."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.partitionBy.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet", partition_by=["date", "region"])
        
        assert result is True
        mock_df.write.partitionBy.assert_called_once_with("date", "region")

    def test_load_overwrite_mode(self, mock_spark_session, test_config):
        """Test loading in overwrite mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet", mode="overwrite")
        
        assert result is True
        mock_df.write.mode.assert_called_once_with("overwrite")

    def test_load_append_mode(self, mock_spark_session, test_config):
        """Test loading in append mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet", mode="append")
        
        assert result is True
        mock_df.write.mode.assert_called_once_with("append")

    def test_load_with_table_name(self, mock_spark_session, test_config):
        """Test loading with table name (path mapping)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        with patch.object(loader, '_build_s3_path', return_value="s3a://bucket/data-lake/countries/"):
            result = loader.load(mock_df, "countries", table_name="countries", format="parquet")
            
            assert result is True

    def test_load_with_s3_path_template(self, mock_spark_session, test_config):
        """Test loading with S3 path template."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "event_date"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.limit = Mock(return_value=mock_df)
        mock_df.collect.return_value = [Mock(event_date=datetime(2024, 1, 1))]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(
            mock_df,
            "countries",
            table_name="countries",
            s3_path_template="{domain}/{table}/{date}/",
            format="parquet"
        )
        
        assert result is True

    def test_load_s3_protocol_conversion(self, mock_spark_session, test_config):
        """Test S3 protocol conversion (s3:// to s3a://)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3://bucket/data/", format="parquet")
        
        assert result is True
        # Verify save was called with s3a:// protocol
        call_args = mock_df.write.save.call_args[0][0]
        assert call_args.startswith("s3a://")

    def test_load_relative_path(self, mock_spark_session, test_config):
        """Test loading with relative path (should prepend bucket)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "data/countries/", format="parquet")
        
        assert result is True
        # Verify save was called with full s3a:// path
        call_args = mock_df.write.save.call_args[0][0]
        assert call_args.startswith("s3a://")

    def test_load_empty_dataframe(self, mock_spark_session, test_config):
        """Test loading empty DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet")
        
        assert result is False

    def test_load_error_handling(self, mock_spark_session, test_config):
        """Test error handling during load."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save.side_effect = Exception("S3 error")
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(mock_df, "s3a://bucket/data/", format="parquet")
        
        assert result is False

    def test_load_with_format_options(self, mock_spark_session, test_config):
        """Test loading with format-specific options."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        result = loader.load(
            mock_df,
            "s3a://bucket/data/",
            format="parquet",
            compression="snappy",
            blockSize=128 * 1024 * 1024
        )
        
        assert result is True
        # Verify options were set
        assert mock_df.write.option.called

    def test_build_s3_path(self, mock_spark_session, test_config):
        """Test building S3 path from template."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "event_date"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.limit = Mock(return_value=mock_df)
        mock_df.collect.return_value = [Mock(event_date=datetime(2024, 1, 15))]
        
        loader = S3Loader(mock_spark_session, test_config)
        path = loader._build_s3_path("{domain}/{table}/{date}/", "customer_events", mock_df)
        
        assert path is not None
        assert "customer" in path or "default" in path
        assert "customer_events" in path or "events" in path
        assert "2024-01-15" in path

    def test_build_s3_path_no_date_column(self, mock_spark_session, test_config):
        """Test building S3 path when no date column exists (should use current date)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.limit = Mock(return_value=mock_df)
        mock_df.collect.return_value = []
        
        loader = S3Loader(mock_spark_session, test_config)
        with patch('src.ownlens.processing.etl.loaders.s3_loader.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2024-01-15"
            path = loader._build_s3_path("{domain}/{table}/{date}/", "countries", mock_df)
            
            assert path is not None
            assert "2024-01-15" in path

    def test_load_with_table_config(self, mock_spark_session, test_config):
        """Test loading with table-specific config."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "date_col"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.partitionBy.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = S3Loader(mock_spark_session, test_config)
        with patch('src.ownlens.processing.etl.loaders.s3_loader.get_table_load_config') as mock_get_config:
            mock_config = Mock()
            mock_config.format = "delta"
            mock_config.mode = "append"
            mock_config.partition_by = ["date_col"]
            mock_config.s3_path_template = "{domain}/{table}/{date}/"
            mock_get_config.return_value = mock_config
            
            with patch.object(loader, '_build_s3_path', return_value="s3a://bucket/data-lake/countries/2024-01-15/"):
                result = loader.load(mock_df, "countries", table_name="countries")
                
                assert result is True
                mock_df.write.format.assert_called_once_with("delta")
                mock_df.write.mode.assert_called_once_with("append")

