"""
Integration Tests for PostgreSQL Loading
=========================================
"""

import pytest


@pytest.mark.integration
class TestPostgreSQLLoading:
    """Integration tests for PostgreSQL loading."""

    def test_load_to_postgresql_integration(self, spark_session, test_config):
        """Test loading data to PostgreSQL (if available)."""
        # Skip if test database is not available
        pytest.skip("Requires test PostgreSQL database")
        
        from src.ownlens.processing.etl.loaders.postgresql_loader import PostgreSQLLoader
        from pyspark.sql import Row
        
        # Create test DataFrame
        test_data = [Row(id="1", name="Test")]
        df = spark_session.createDataFrame(test_data)
        
        loader = PostgreSQLLoader(spark_session, test_config)
        result = loader.load(df, "test_table", mode="append")
        
        assert result is not None

