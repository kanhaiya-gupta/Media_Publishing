"""
Unit Tests for PostgreSQLLoader
================================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType

from src.ownlens.processing.etl.loaders.postgresql_loader import PostgreSQLLoader
from src.ownlens.processing.etl.utils.config import ETLConfig


class TestPostgreSQLLoader:
    """Test PostgreSQLLoader class."""

    def test_init(self, mock_spark_session, test_config):
        """Test loader initialization."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is test_config
        assert loader.jdbc_url is not None
        assert "postgresql" in loader.jdbc_url.lower() or "postgres" in loader.jdbc_url.lower()
        assert loader.jdbc_properties is not None
        assert "user" in loader.jdbc_properties or "driver" in loader.jdbc_properties

    def test_init_without_config(self, mock_spark_session):
        """Test loader initialization without config."""
        loader = PostgreSQLLoader(mock_spark_session)
        
        assert loader.spark is mock_spark_session
        assert loader.etl_config is not None
        assert loader.jdbc_url is not None

    def test_load_append_mode(self, mock_spark_session, test_config):
        """Test loading data in append mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df), \
             patch.object(loader, '_insert_with_casting', return_value=True):
            result = loader.load(mock_df, "countries", mode="append")
            
            assert result is True

    def test_load_overwrite_mode(self, mock_spark_session, test_config):
        """Test loading data in overwrite mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 20
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df):
            result = loader.load(mock_df, "countries", mode="overwrite")
            
            assert result is not None
            mock_df.write.mode.assert_called_once_with("overwrite")

    def test_load_upsert_mode(self, mock_spark_session, test_config):
        """Test loading data in upsert mode."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 15
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df), \
             patch.object(loader, '_upsert', return_value=True):
            result = loader.load(mock_df, "countries", mode="upsert", primary_key=["id"])
            
            assert result is True

    def test_load_upsert_mode_fallback(self, mock_spark_session, test_config):
        """Test upsert mode falling back to append when upsert fails."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 15
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df), \
             patch.object(loader, '_upsert', return_value=False), \
             patch.object(loader, '_insert_with_casting', return_value=True):
            result = loader.load(mock_df, "countries", mode="upsert", primary_key=["id"])
            
            assert result is True

    def test_load_empty_dataframe(self, mock_spark_session, test_config):
        """Test loading empty DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 0
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader.load(mock_df, "countries")
        
        assert result is False

    def test_load_with_batch_size(self, mock_spark_session, test_config):
        """Test loading with custom batch size."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df), \
             patch.object(loader, '_insert_with_casting', return_value=True):
            result = loader.load(mock_df, "countries", batch_size=5000)
            
            assert result is True

    def test_load_with_column_filtering(self, mock_spark_session, test_config):
        """Test loading with column filtering (DataFrame has extra columns)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name", "extra_col"]
        mock_df.select = Mock(return_value=mock_df)
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df), \
             patch.object(loader, '_insert_with_casting', return_value=True):
            result = loader.load(mock_df, "countries", mode="append")
            
            assert result is True
            # Verify select was called to filter columns
            mock_df.select.assert_called()

    def test_load_no_common_columns(self, mock_spark_session, test_config):
        """Test loading when DataFrame and table have no common columns."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["col1", "col2"]
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}):
            result = loader.load(mock_df, "countries")
            
            assert result is False

    def test_load_error_handling(self, mock_spark_session, test_config):
        """Test error handling during load."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.select = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save.side_effect = Exception("Connection error")
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        with patch.object(loader, '_get_table_columns', return_value={"id", "name"}), \
             patch.object(loader, '_map_foreign_keys', return_value=mock_df), \
             patch.object(loader, '_cast_columns_for_postgresql', return_value=mock_df):
            result = loader.load(mock_df, "countries", mode="overwrite")
            
            assert result is False

    def test_get_table_columns(self, mock_spark_session, test_config):
        """Test getting table columns."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        # Mock Spark read
        mock_df = Mock()
        mock_df.collect.return_value = [("id",), ("name",)]
        mock_spark_session.read = Mock()
        mock_spark_session.read.format.return_value = mock_spark_session.read
        mock_spark_session.read.options.return_value = mock_spark_session.read
        mock_spark_session.read.load.return_value = mock_df
        
        columns = loader._get_table_columns("countries")
        
        assert columns is not None
        assert len(columns) > 0

    def test_get_table_columns_error_handling(self, mock_spark_session, test_config):
        """Test getting table columns when query fails."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        # Mock Spark read failure
        mock_spark_session.read = Mock()
        mock_spark_session.read.format.return_value = mock_spark_session.read
        mock_spark_session.read.options.return_value = mock_spark_session.read
        mock_spark_session.read.load.side_effect = Exception("Query failed")
        
        columns = loader._get_table_columns("countries")
        
        # Should return None or empty set on error
        assert columns is None or len(columns) == 0

    def test_map_foreign_keys(self, mock_spark_session, test_config):
        """Test foreign key mapping."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "company_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._map_foreign_keys(mock_df, "countries")
        
        assert result is not None

    def test_cast_columns_for_postgresql(self, mock_spark_session, test_config):
        """Test column casting for PostgreSQL."""
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "binary_col"]
        mock_df.schema = StructType([
            StructField("id", StringType()),
            StructField("binary_col", BinaryType())
        ])
        mock_df.withColumn = Mock(return_value=mock_df)
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._cast_columns_for_postgresql(mock_df, "countries")
        
        assert result is not None
        # Should convert BinaryType columns
        assert mock_df.withColumn.called

    def test_get_unique_constraints(self, mock_spark_session, test_config):
        """Test getting unique constraints."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        # Mock Spark read
        mock_df = Mock()
        mock_df.collect.return_value = [
            ("pk_constraint", "p", ["id"]),
            ("uk_constraint", "u", ["name"])
        ]
        mock_spark_session.read = Mock()
        mock_spark_session.read.format.return_value = mock_spark_session.read
        mock_spark_session.read.options.return_value = mock_spark_session.read
        mock_spark_session.read.load.return_value = mock_df
        
        constraints = loader._get_unique_constraints("countries")
        
        assert constraints is not None
        assert len(constraints) > 0

    def test_upsert_success(self, mock_spark_session, test_config):
        """Test successful upsert operation."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._upsert(mock_df, "countries", ["id"], "update", 10000)
        
        assert result is True

    def test_upsert_error_handling(self, mock_spark_session, test_config):
        """Test upsert error handling."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save.side_effect = Exception("Upsert failed")
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._upsert(mock_df, "countries", ["id"], "update", 10000)
        
        assert result is False

    def test_insert_with_casting_success(self, mock_spark_session, test_config):
        """Test successful insert with casting."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save = Mock()
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._insert_with_casting(mock_df, "countries", 10000)
        
        assert result is True

    def test_insert_with_casting_error_handling(self, mock_spark_session, test_config):
        """Test insert with casting error handling."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.save.side_effect = Exception("Insert failed")
        
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        result = loader._insert_with_casting(mock_df, "countries", 10000)
        
        assert result is False

