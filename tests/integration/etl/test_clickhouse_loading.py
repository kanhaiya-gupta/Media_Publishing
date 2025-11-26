"""
Integration Tests for ClickHouse Loading
========================================
"""

import pytest


@pytest.mark.integration
class TestClickHouseLoading:
    """Integration tests for ClickHouse loading."""

    def test_load_to_clickhouse_integration(self, spark_session, test_config):
        """Test loading data to ClickHouse (if available)."""
        # Skip if test ClickHouse is not available
        pytest.skip("Requires test ClickHouse database")
        
        from src.ownlens.processing.etl.loaders.clickhouse_loader import ClickHouseLoader
        from pyspark.sql import Row
        
        # Create test DataFrame
        test_data = [Row(id="1", name="Test")]
        df = spark_session.createDataFrame(test_data)
        
        loader = ClickHouseLoader(spark_session, test_config)
        result = loader.load(df, "test_table")
        
        assert result is not None

