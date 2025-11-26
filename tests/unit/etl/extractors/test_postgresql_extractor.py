"""
Unit Tests for PostgreSQLExtractor
==================================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame

from src.ownlens.processing.etl.extractors.postgresql_extractor import (
    PostgreSQLExtractor,
    POSTGRESQL_TABLES,
)
from src.ownlens.processing.etl.utils.config import ETLConfig


class TestPostgreSQLExtractor:
    """Test PostgreSQLExtractor class."""

    def test_init(self, mock_spark_session, test_config):
        """Test extractor initialization."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        assert extractor.spark is mock_spark_session
        assert extractor.etl_config is test_config
        assert extractor.jdbc_url is not None
        assert extractor.jdbc_properties is not None

    def test_init_without_config(self, mock_spark_session):
        """Test extractor initialization without config."""
        extractor = PostgreSQLExtractor(mock_spark_session)
        
        assert extractor.spark is mock_spark_session
        assert extractor.etl_config is not None

    def test_extract_table_success(self, mock_spark_session, test_config):
        """Test successful table extraction."""
        # Mock Spark read
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries")
        
        assert df is not None
        mock_reader.format.assert_called_once_with("jdbc")
        mock_reader.load.assert_called_once()

    def test_extract_table_with_schema(self, mock_spark_session, test_config):
        """Test table extraction with schema."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 5
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", schema="public")
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_table_with_partitioning(self, mock_spark_session, test_config):
        """Test table extraction with partitioning."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(
            table="countries",
            partition_column="id",
            lower_bound=1,
            upper_bound=100,
            num_partitions=4
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_query_success(self, mock_spark_session, test_config):
        """Test successful query extraction."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 20
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(query="SELECT * FROM countries WHERE id > 10")
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_no_table_or_query(self, mock_spark_session, test_config):
        """Test extraction without table or query."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        with pytest.raises(ValueError, match="Either 'table' or 'query' must be provided"):
            extractor.extract()

    def test_extract_incremental(self, mock_spark_session, test_config):
        """Test incremental extraction."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 15
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract_incremental(
            table="customer_events",
            timestamp_column="created_at",
            last_timestamp="2024-01-01T00:00:00"
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_incremental_no_last_timestamp(self, mock_spark_session, test_config):
        """Test incremental extraction without last timestamp."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract_incremental(
            table="customer_events",
            timestamp_column="created_at"
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_error_handling(self, mock_spark_session, test_config):
        """Test error handling during extraction."""
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.side_effect = Exception("Connection error")
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        with pytest.raises(Exception, match="Connection error"):
            extractor.extract(table="countries")

    def test_extract_with_where_clause(self, mock_spark_session, test_config):
        """Test table extraction with WHERE clause."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 5
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", where="id > 10")
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_with_limit(self, mock_spark_session, test_config):
        """Test table extraction with LIMIT."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", limit=10)
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_with_custom_query_options(self, mock_spark_session, test_config):
        """Test extraction with custom query options."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 15
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(
            query="SELECT * FROM countries",
            fetchsize=1000,
            isolationLevel="READ_COMMITTED"
        )
        
        assert df is not None
        # Verify options were set
        assert mock_reader.options.called

    def test_extract_incremental_with_date_range(self, mock_spark_session, test_config):
        """Test incremental extraction with date range."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 25
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract_incremental(
            table="customer_events",
            timestamp_column="created_at",
            last_timestamp="2024-01-01T00:00:00",
            end_timestamp="2024-01-31T23:59:59"
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_incremental_with_timezone(self, mock_spark_session, test_config):
        """Test incremental extraction with timezone handling."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 30
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract_incremental(
            table="customer_events",
            timestamp_column="created_at",
            last_timestamp="2024-01-01T00:00:00Z",
            timezone="UTC"
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_with_table_and_query_error(self, mock_spark_session, test_config):
        """Test extraction error when both table and query are provided."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        with pytest.raises(ValueError, match="Cannot specify both 'table' and 'query'"):
            extractor.extract(table="countries", query="SELECT * FROM countries")

    def test_extract_with_invalid_partitioning(self, mock_spark_session, test_config):
        """Test extraction with invalid partitioning parameters."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        # Should handle gracefully or raise appropriate error
        with pytest.raises((ValueError, TypeError)):
            extractor.extract(
                table="countries",
                partition_column="id",
                lower_bound=None,  # Invalid
                upper_bound=100,
                num_partitions=4
            )

    def test_extract_table_with_full_qualified_name(self, mock_spark_session, test_config):
        """Test extraction with fully qualified table name (schema.table)."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="public.countries")
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_query_with_parameters(self, mock_spark_session, test_config):
        """Test query extraction with parameterized query."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 20
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(query="SELECT * FROM countries WHERE id > ? AND name = ?")
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_incremental_with_null_timestamp(self, mock_spark_session, test_config):
        """Test incremental extraction when timestamp column has nulls."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 40
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract_incremental(
            table="customer_events",
            timestamp_column="created_at",
            include_nulls=True
        )
        
        assert df is not None
        mock_reader.load.assert_called_once()

    def test_extract_with_column_selection(self, mock_spark_session, test_config):
        """Test extraction with specific column selection."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        mock_df.columns = ["id", "name"]
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", columns=["id", "name"])
        
        assert df is not None
        assert "id" in df.columns
        assert "name" in df.columns

    def test_extract_with_custom_driver(self, mock_spark_session, test_config):
        """Test extraction with custom JDBC driver."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", driver="org.postgresql.Driver")
        
        assert df is not None
        # Verify driver option was set
        assert mock_reader.options.called

    def test_extract_with_connection_timeout(self, mock_spark_session, test_config):
        """Test extraction with connection timeout."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10
        
        mock_reader = Mock()
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        mock_spark_session.read = Mock(return_value=mock_reader)
        
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        df = extractor.extract(table="countries", connection_timeout=30)
        
        assert df is not None
        # Verify timeout option was set
        assert mock_reader.options.called


class TestPostgreSQLTables:
    """Test PostgreSQL tables list."""

    def test_postgresql_tables_not_empty(self):
        """Test that POSTGRESQL_TABLES is not empty."""
        assert len(POSTGRESQL_TABLES) > 0

    def test_postgresql_tables_contains_base_tables(self):
        """Test that POSTGRESQL_TABLES contains base tables."""
        assert "countries" in POSTGRESQL_TABLES
        assert "cities" in POSTGRESQL_TABLES
        assert "users" in POSTGRESQL_TABLES

    def test_postgresql_tables_contains_customer_tables(self):
        """Test that POSTGRESQL_TABLES contains customer domain tables."""
        assert "customer_sessions" in POSTGRESQL_TABLES
        assert "customer_events" in POSTGRESQL_TABLES

    def test_postgresql_tables_contains_editorial_tables(self):
        """Test that POSTGRESQL_TABLES contains editorial domain tables."""
        assert "editorial_articles" in POSTGRESQL_TABLES
        assert "editorial_authors" in POSTGRESQL_TABLES

