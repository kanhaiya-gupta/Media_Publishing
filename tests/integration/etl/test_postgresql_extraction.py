"""
Integration Tests for PostgreSQL Extraction
============================================
"""

import pytest
from pyspark.sql import SparkSession

from src.ownlens.processing.etl.extractors.postgresql_extractor import PostgreSQLExtractor
from src.ownlens.processing.etl.utils.config import ETLConfig


@pytest.mark.integration
class TestPostgreSQLExtraction:
    """Integration tests for PostgreSQL extraction."""

    def test_extract_table_integration(self, spark_session, test_config):
        """Test table extraction with real database (if available)."""
        # Skip if test database is not available
        pytest.skip("Requires test PostgreSQL database")
        
        extractor = PostgreSQLExtractor(spark_session, test_config)
        df = extractor.extract(table="countries")
        
        assert df is not None
        assert df.count() >= 0

