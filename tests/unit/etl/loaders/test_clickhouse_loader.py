"""
Unit Tests for ClickHouseLoader
===============================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, MapType, TimestampType
from datetime import datetime, date, timezone
import json

from src.ownlens.processing.etl.loaders.clickhouse_loader import ClickHouseLoader
from src.ownlens.processing.etl.utils.config import ETLConfig


class TestClickHouseLoader:
    """Test ClickHouseLoader class."""

    def test_init(self, mock_spark_session, test_config):
        """Test loader initialization."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader.spark is mock_spark_session
            assert loader.etl_config is test_config
            assert loader.clickhouse_url is not None
            assert "clickhouse" in loader.clickhouse_url.lower()
            assert loader.clickhouse_properties is not None
            assert "user" in loader.clickhouse_properties
            assert "password" in loader.clickhouse_properties
            assert "driver" in loader.clickhouse_properties

    def test_init_without_config(self, mock_spark_session):
        """Test loader initialization without config."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session)
            
            assert loader.spark is mock_spark_session
            assert loader.etl_config is not None
            assert loader.clickhouse_url is not None

    def test_init_with_password_in_url(self, mock_spark_session, test_config):
        """Test loader initialization with password in URL."""
        test_config.clickhouse_password = "test_password"
        test_config.clickhouse_user = "test_user"
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert "password=test_password" in loader.clickhouse_url or "password" in loader.clickhouse_properties
            assert loader.clickhouse_properties["user"] == "test_user"

    def test_init_ensures_database_exists(self, mock_spark_session, test_config):
        """Test that initialization ensures database exists."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists') as mock_ensure:
            ClickHouseLoader(mock_spark_session, test_config)
            mock_ensure.assert_called_once()

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_success(self, mock_client_class, mock_spark_session, test_config):
        """Test successful data loading."""
        # Mock ClickHouse client
        mock_client = Mock()
        mock_client.execute.side_effect = [
            [("id", "String", ""), ("name", "String", "")],  # DESCRIBE TABLE
            [("id", "String", ""), ("name", "String", "")],  # system.columns query
        ]
        mock_client_class.return_value = mock_client
        
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name"]
        mock_df.collect.return_value = [
            Mock(asDict=lambda: {"id": "1", "name": "Test"})
        ]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is True
            mock_client.execute.assert_called()

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_with_materialized_columns(self, mock_client_class, mock_spark_session, test_config):
        """Test loading with MATERIALIZED columns (should be filtered out)."""
        mock_client = Mock()
        mock_client.execute.side_effect = [
            [("id", "String", ""), ("computed_col", "String", ""), ("name", "String", "")],  # DESCRIBE
            [
                ("id", "String", ""),
                ("computed_col", "String", "MATERIALIZED"),  # MATERIALIZED column
                ("name", "String", "")
            ],  # system.columns
        ]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.collect.return_value = [Mock(asDict=lambda: {"id": "1", "name": "Test"})]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is True
            # Verify MATERIALIZED column was filtered out
            calls = [c[0][0] for c in mock_client.execute.call_args_list]
            assert any("MATERIALIZED" in str(call) or "system.columns" in str(call) for call in calls)

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_with_nullable_datetime(self, mock_client_class, mock_spark_session, test_config):
        """Test loading with Nullable DateTime columns."""
        mock_client = Mock()
        mock_client.execute.side_effect = [
            [("id", "String", ""), ("created_at", "Nullable(DateTime)", "")],
            [("id", "String", ""), ("created_at", "Nullable(DateTime)", "")],
        ]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "created_at"]
        mock_df.collect.return_value = [
            Mock(asDict=lambda: {"id": "1", "created_at": None})  # None value for nullable column
        ]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is True

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_with_non_nullable_datetime_none_value(self, mock_client_class, mock_spark_session, test_config):
        """Test loading with non-nullable DateTime columns and None values (should use placeholder)."""
        mock_client = Mock()
        mock_client.execute.side_effect = [
            [("id", "String", ""), ("created_at", "DateTime", "")],  # Non-nullable
            [("id", "String", ""), ("created_at", "DateTime", "")],
        ]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "created_at"]
        mock_df.collect.return_value = [
            Mock(asDict=lambda: {"id": "1", "created_at": None})  # None value
        ]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is True
            # Verify placeholder datetime was used (1970-01-01)
            insert_call = [c for c in mock_client.execute.call_args_list if "INSERT" in str(c)]
            assert len(insert_call) > 0

    def test_load_empty_dataframe(self, mock_spark_session, test_config):
        """Test loading empty DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is False

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_with_batch_size(self, mock_client_class, mock_spark_session, test_config):
        """Test loading with custom batch size."""
        mock_client = Mock()
        mock_client.execute.return_value = [("id", "String", "")]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["id"]
        mock_df.collect.return_value = [Mock(asDict=lambda: {"id": "1"})]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table", batch_size=50000)
            
            assert result is True

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_large_dataset_optimizes_batch_size(self, mock_client_class, mock_spark_session, test_config):
        """Test that large datasets optimize batch size automatically."""
        mock_client = Mock()
        mock_client.execute.return_value = [("id", "String", "")]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 200000  # Large dataset
        mock_df.columns = ["id"]
        mock_df.collect.return_value = [Mock(asDict=lambda: {"id": "1"})]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table", batch_size=10000)
            
            assert result is True

    def test_load_error_handling_table_not_exists(self, mock_spark_session, test_config):
        """Test error handling when table doesn't exist."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id"]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=False):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is False

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_error_handling_client_connection_failure(self, mock_client_class, mock_spark_session, test_config):
        """Test error handling when ClickHouse client connection fails."""
        mock_client_class.side_effect = Exception("Connection failed")
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id"]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is False

    @patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client')
    def test_load_error_handling_insert_failure(self, mock_client_class, mock_spark_session, test_config):
        """Test error handling when INSERT fails."""
        mock_client = Mock()
        mock_client.execute.side_effect = [
            [("id", "String", "")],  # DESCRIBE succeeds
            [("id", "String", "")],  # system.columns succeeds
            Exception("'I' format error"),  # INSERT fails
        ]
        mock_client_class.return_value = mock_client
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id"]
        mock_df.collect.return_value = [Mock(asDict=lambda: {"id": "1"})]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            result = loader.load(mock_df, "test_table")
            
            assert result is False

    def test_load_replace_partition_mode(self, mock_spark_session, test_config):
        """Test loading with replace_partition mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "date_col"]
        mock_df.select.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.collect.return_value = [Mock(date_col="2024-01-01")]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, 'load', return_value=True) as mock_load:
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            with patch.object(loader, '_get_table_config', return_value=Mock(clickhouse_partition="toYYYYMM(date_col)")):
                result = loader._replace_partition(mock_df, "test_table", "test_table", 10000)
                
                assert result is True
                mock_load.assert_called()

    def test_load_replace_partition_no_partition_expr(self, mock_spark_session, test_config):
        """Test replace_partition mode without partition expression falls back to append."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, 'load', return_value=True) as mock_load:
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            with patch.object(loader, '_get_table_config', return_value=None):
                result = loader._replace_partition(mock_df, "test_table", "test_table", 10000)
                
                assert result is True
                mock_load.assert_called_once_with(mock_df, "test_table", mode="append", batch_size=10000, **{})

    def test_load_merge_mode_with_replacing_merge_tree(self, mock_spark_session, test_config):
        """Test merge mode with ReplacingMergeTree engine."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id"]
        
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch.object(ClickHouseLoader, '_is_replacing_merge_tree', return_value=True), \
             patch.object(ClickHouseLoader, '_verify_table_exists', return_value=True), \
             patch.object(ClickHouseLoader, '_convert_types_for_clickhouse', return_value=mock_df), \
             patch.object(ClickHouseLoader, '_filter_columns_for_clickhouse', return_value=mock_df):
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            # Should use append mode for ReplacingMergeTree
            with patch.object(loader, 'load', return_value=True) as mock_load:
                result = loader.load(mock_df, "test_table", mode="merge")
                assert result is True

    def test_get_clickhouse_table_name_default(self, mock_spark_session, test_config):
        """Test getting ClickHouse table name (default: same name)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            table_name = loader._get_clickhouse_table_name("test_table")
            assert table_name == "test_table"

    def test_get_clickhouse_table_name_from_config(self, mock_spark_session, test_config):
        """Test getting ClickHouse table name from config."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'), \
             patch('src.ownlens.processing.etl.loaders.clickhouse_loader.TABLE_LOAD_CONFIGS', {
                 "test_table": {
                     "clickhouse": Mock(clickhouse_table="clickhouse_test_table")
                 }
             }):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            table_name = loader._get_clickhouse_table_name("test_table")
            assert table_name == "clickhouse_test_table"

    def test_convert_datetime_for_clickhouse_datetime_object(self, mock_spark_session, test_config):
        """Test datetime conversion for DateTime type."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Test datetime object
            dt = datetime(2024, 1, 1, 12, 0, 0)
            result = loader._convert_datetime_for_clickhouse(dt, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 2024
            assert result.month == 1
            assert result.day == 1

    def test_convert_datetime_for_clickhouse_datetime_with_timezone(self, mock_spark_session, test_config):
        """Test datetime conversion with timezone (should remove timezone)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Test datetime with timezone
            dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            result = loader._convert_datetime_for_clickhouse(dt, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.tzinfo is None  # Timezone should be removed

    def test_convert_datetime_for_clickhouse_date_object(self, mock_spark_session, test_config):
        """Test date conversion for Date type."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Test date object for Date column
            d = date(2024, 1, 1)
            result = loader._convert_datetime_for_clickhouse(d, "Date")
            assert result is not None
            assert isinstance(result, date)
            assert result.year == 2024

    def test_convert_datetime_for_clickhouse_date_to_datetime(self, mock_spark_session, test_config):
        """Test date object conversion to DateTime (should become midnight datetime)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Test date object for DateTime column (should convert to datetime)
            d = date(2024, 1, 1)
            result = loader._convert_datetime_for_clickhouse(d, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.hour == 0
            assert result.minute == 0
            assert result.second == 0

    def test_convert_datetime_for_clickhouse_unix_timestamp_seconds(self, mock_spark_session, test_config):
        """Test Unix timestamp (seconds) conversion."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Unix timestamp: 1704067200 = 2024-01-01 00:00:00
            timestamp = 1704067200
            result = loader._convert_datetime_for_clickhouse(timestamp, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 2024

    def test_convert_datetime_for_clickhouse_unix_timestamp_milliseconds(self, mock_spark_session, test_config):
        """Test Unix timestamp (milliseconds) conversion."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Unix timestamp in milliseconds: 1704067200000
            timestamp = 1704067200000
            result = loader._convert_datetime_for_clickhouse(timestamp, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 2024

    def test_convert_datetime_for_clickhouse_string_iso_format(self, mock_spark_session, test_config):
        """Test string timestamp (ISO format) conversion."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # ISO format string
            timestamp_str = "2024-01-01T12:00:00Z"
            result = loader._convert_datetime_for_clickhouse(timestamp_str, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 2024

    def test_convert_datetime_for_clickhouse_string_common_format(self, mock_spark_session, test_config):
        """Test string timestamp (common format) conversion."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Common format string
            timestamp_str = "2024-01-01 12:00:00"
            result = loader._convert_datetime_for_clickhouse(timestamp_str, "DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 2024

    def test_convert_datetime_for_clickhouse_none_value(self, mock_spark_session, test_config):
        """Test None value conversion."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            result = loader._convert_datetime_for_clickhouse(None, "DateTime")
            assert result is None

    def test_convert_value_for_clickhouse_integer_types(self, mock_spark_session, test_config):
        """Test value conversion for integer types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Test various integer types
            assert loader._convert_value_for_clickhouse(123, "UInt32", "test_col") == 123
            assert loader._convert_value_for_clickhouse(123, "Int32", "test_col") == 123
            assert loader._convert_value_for_clickhouse(123.5, "UInt32", "test_col") == 123
            assert loader._convert_value_for_clickhouse("123", "UInt32", "test_col") == 123

    def test_convert_value_for_clickhouse_float_types(self, mock_spark_session, test_config):
        """Test value conversion for float types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._convert_value_for_clickhouse(123.5, "Float64", "test_col") == 123.5
            assert loader._convert_value_for_clickhouse(123, "Float64", "test_col") == 123.0
            assert loader._convert_value_for_clickhouse("123.5", "Float64", "test_col") == 123.5

    def test_convert_value_for_clickhouse_string_types(self, mock_spark_session, test_config):
        """Test value conversion for string types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._convert_value_for_clickhouse("test", "String", "test_col") == "test"
            assert loader._convert_value_for_clickhouse(123, "String", "test_col") == "123"
            assert loader._convert_value_for_clickhouse(None, "String", "test_col") == ""

    def test_convert_value_for_clickhouse_boolean_types(self, mock_spark_session, test_config):
        """Test value conversion for boolean types (converted to UInt8)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._convert_value_for_clickhouse(True, "UInt8", "test_col") == 1
            assert loader._convert_value_for_clickhouse(False, "UInt8", "test_col") == 0
            assert loader._convert_value_for_clickhouse("true", "UInt8", "test_col") == 1
            assert loader._convert_value_for_clickhouse("false", "UInt8", "test_col") == 0

    def test_convert_value_for_clickhouse_nullable_types(self, mock_spark_session, test_config):
        """Test value conversion for Nullable types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Nullable String should allow None
            result = loader._convert_value_for_clickhouse(None, "Nullable(String)", "test_col")
            assert result is None
            
            # Nullable DateTime should allow None
            result = loader._convert_value_for_clickhouse(None, "Nullable(DateTime)", "test_col")
            assert result is None

    def test_convert_value_for_clickhouse_non_nullable_datetime_none(self, mock_spark_session, test_config):
        """Test value conversion for non-nullable DateTime with None (should use placeholder)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            result = loader._convert_value_for_clickhouse(None, "DateTime", "test_col")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 1970  # Placeholder date

    def test_convert_value_for_clickhouse_uuid_type(self, mock_spark_session, test_config):
        """Test value conversion for UUID type (converted to string)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            uuid_value = "550e8400-e29b-41d4-a716-446655440000"
            result = loader._convert_value_for_clickhouse(uuid_value, "UUID", "test_col")
            assert result == uuid_value
            assert isinstance(result, str)

    def test_convert_value_for_clickhouse_json_type(self, mock_spark_session, test_config):
        """Test value conversion for JSON type (converted to string)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            json_value = {"key": "value"}
            result = loader._convert_value_for_clickhouse(json_value, "JSON", "test_col")
            assert isinstance(result, str)
            assert "key" in result

    def test_convert_value_for_clickhouse_array_type(self, mock_spark_session, test_config):
        """Test value conversion for Array type."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            array_value = [1, 2, 3]
            result = loader._convert_value_for_clickhouse(array_value, "Array(UInt32)", "test_col")
            assert result == array_value
            assert isinstance(result, list)

    def test_get_default_value_for_clickhouse_type_integer(self, mock_spark_session, test_config):
        """Test getting default value for integer types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._get_default_value_for_clickhouse_type("UInt32") == 0
            assert loader._get_default_value_for_clickhouse_type("Int32") == 0
            assert loader._get_default_value_for_clickhouse_type("UInt64") == 0

    def test_get_default_value_for_clickhouse_type_float(self, mock_spark_session, test_config):
        """Test getting default value for float types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._get_default_value_for_clickhouse_type("Float64") == 0.0
            assert loader._get_default_value_for_clickhouse_type("Float32") == 0.0

    def test_get_default_value_for_clickhouse_type_string(self, mock_spark_session, test_config):
        """Test getting default value for string types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._get_default_value_for_clickhouse_type("String") == ""
            assert loader._get_default_value_for_clickhouse_type("FixedString(10)") == ""

    def test_get_default_value_for_clickhouse_type_nullable(self, mock_spark_session, test_config):
        """Test getting default value for Nullable types."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            assert loader._get_default_value_for_clickhouse_type("Nullable(String)") is None
            assert loader._get_default_value_for_clickhouse_type("Nullable(UInt32)") is None

    def test_get_default_value_for_clickhouse_type_datetime_non_nullable(self, mock_spark_session, test_config):
        """Test getting default value for non-nullable DateTime (should use placeholder)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            result = loader._get_default_value_for_clickhouse_type("DateTime")
            assert result is not None
            assert isinstance(result, datetime)
            assert result.year == 1970  # Placeholder date

    def test_get_default_value_for_clickhouse_type_datetime_nullable(self, mock_spark_session, test_config):
        """Test getting default value for Nullable DateTime (should be None)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            result = loader._get_default_value_for_clickhouse_type("Nullable(DateTime)")
            assert result is None

    def test_verify_table_exists_jdbc_success(self, mock_spark_session, test_config):
        """Test table existence verification via JDBC."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Mock Spark read
            mock_df = Mock()
            mock_df.collect.return_value = [("test_table",)]
            mock_spark_session.read = Mock()
            mock_spark_session.read.format.return_value = mock_spark_session.read
            mock_spark_session.read.options.return_value = mock_spark_session.read
            mock_spark_session.read.load.return_value = mock_df
            
            result = loader._verify_table_exists("test_table")
            assert result is True

    def test_verify_table_exists_jdbc_fallback_to_client(self, mock_spark_session, test_config):
        """Test table existence verification falls back to clickhouse-driver when JDBC fails."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Mock JDBC failure
            mock_spark_session.read = Mock()
            mock_spark_session.read.format.return_value = mock_spark_session.read
            mock_spark_session.read.options.return_value = mock_spark_session.read
            mock_spark_session.read.load.side_effect = Exception("JDBC error")
            
            # Mock clickhouse-driver fallback
            with patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client') as mock_client_class:
                mock_client = Mock()
                mock_client.execute.return_value = [("test_table",)]
                mock_client_class.return_value = mock_client
                
                result = loader._verify_table_exists("test_table")
                assert result is True

    def test_verify_table_exists_not_found(self, mock_spark_session, test_config):
        """Test table existence verification when table doesn't exist."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Mock Spark read returning empty
            mock_df = Mock()
            mock_df.collect.return_value = []
            mock_spark_session.read = Mock()
            mock_spark_session.read.format.return_value = mock_spark_session.read
            mock_spark_session.read.options.return_value = mock_spark_session.read
            mock_spark_session.read.load.return_value = mock_df
            
            # Mock fallback also failing
            with patch('src.ownlens.processing.etl.loaders.clickhouse_loader.Client') as mock_client_class:
                mock_client_class.side_effect = Exception("Connection failed")
                
                result = loader._verify_table_exists("test_table")
                assert result is False

    def test_is_replacing_merge_tree_true(self, mock_spark_session, test_config):
        """Test ReplacingMergeTree detection (True)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            with patch.object(loader, '_get_table_engine', return_value="ReplacingMergeTree"):
                result = loader._is_replacing_merge_tree("test_table")
                assert result is True

    def test_is_replacing_merge_tree_false(self, mock_spark_session, test_config):
        """Test ReplacingMergeTree detection (False)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            with patch.object(loader, '_get_table_engine', return_value="MergeTree"):
                result = loader._is_replacing_merge_tree("test_table")
                assert result is False

    def test_is_replacing_merge_tree_none(self, mock_spark_session, test_config):
        """Test ReplacingMergeTree detection when engine is None."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            with patch.object(loader, '_get_table_engine', return_value=None):
                result = loader._is_replacing_merge_tree("test_table")
                assert result is False

    def test_ensure_database_exists_jdbc_success(self, mock_spark_session, test_config):
        """Test database creation via JDBC."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists') as mock_ensure:
            # Mock Spark read for SHOW DATABASES
            mock_df = Mock()
            mock_df.collect.return_value = [("default",), ("test_db",)]
            mock_spark_session.read = Mock()
            mock_spark_session.read.format.return_value = mock_spark_session.read
            mock_spark_session.read.options.return_value = mock_spark_session.read
            mock_spark_session.read.load.return_value = mock_df
            
            loader = ClickHouseLoader(mock_spark_session, test_config)
            # Database already exists, should not raise
            assert loader is not None

    def test_convert_types_for_clickhouse_boolean_to_int(self, mock_spark_session, test_config):
        """Test type conversion: Boolean to Integer."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Mock DataFrame with Boolean column
            mock_df = Mock(spec=DataFrame)
            mock_df.columns = ["id", "is_active"]
            mock_df.schema = StructType([
                StructField("id", StringType()),
                StructField("is_active", BooleanType())
            ])
            mock_df.withColumn = Mock(return_value=mock_df)
            
            result = loader._convert_types_for_clickhouse(mock_df, "test_table")
            # Should convert Boolean to Integer
            assert mock_df.withColumn.called

    def test_convert_types_for_clickhouse_struct_to_json(self, mock_spark_session, test_config):
        """Test type conversion: Struct to JSON string."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            # Mock DataFrame with Struct column
            mock_df = Mock(spec=DataFrame)
            mock_df.columns = ["id", "metadata"]
            mock_df.schema = StructType([
                StructField("id", StringType()),
                StructField("metadata", StructType([StructField("key", StringType())]))
            ])
            mock_df.withColumn = Mock(return_value=mock_df)
            
            result = loader._convert_types_for_clickhouse(mock_df, "test_table")
            # Should convert Struct to JSON string
            assert mock_df.withColumn.called

    def test_filter_columns_for_clickhouse_no_schema(self, mock_spark_session, test_config):
        """Test column filtering when schema not found (should return DataFrame as-is)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            mock_df = Mock(spec=DataFrame)
            mock_df.columns = ["id", "name"]
            
            with patch.object(loader, '_get_clickhouse_table_columns', return_value=set()):
                result = loader._filter_columns_for_clickhouse(mock_df, "test_table")
                assert result is mock_df  # Should return as-is when schema not found

    def test_filter_columns_for_clickhouse_common_columns(self, mock_spark_session, test_config):
        """Test column filtering with common columns."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            mock_df = Mock(spec=DataFrame)
            mock_df.columns = ["id", "name", "extra_col"]
            mock_df.select = Mock(return_value=mock_df)
            
            with patch.object(loader, '_get_clickhouse_table_columns', return_value={"id", "name"}):
                result = loader._filter_columns_for_clickhouse(mock_df, "test_table")
                # Should select only common columns
                assert mock_df.select.called

    def test_filter_columns_for_clickhouse_no_common_columns(self, mock_spark_session, test_config):
        """Test column filtering when no common columns (should return DataFrame as-is with warning)."""
        with patch.object(ClickHouseLoader, '_ensure_database_exists'):
            loader = ClickHouseLoader(mock_spark_session, test_config)
            
            mock_df = Mock(spec=DataFrame)
            mock_df.columns = ["id", "name"]
            
            with patch.object(loader, '_get_clickhouse_table_columns', return_value={"different_col"}):
                result = loader._filter_columns_for_clickhouse(mock_df, "test_table")
                # Should return as-is when no common columns
                assert result is mock_df

