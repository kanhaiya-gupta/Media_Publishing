"""
Integration Tests for S3 Loading
==================================
"""

import pytest


@pytest.mark.integration
class TestS3Loading:
    """Integration tests for S3 loading."""

    def test_load_to_s3_integration(self, spark_session, test_config):
        """Test loading data to S3/MinIO (if available)."""
        # Skip if test S3 is not available
        pytest.skip("Requires test S3/MinIO bucket")
        
        from src.ownlens.processing.etl.loaders.s3_loader import S3Loader
        from pyspark.sql import Row
        
        # Create test DataFrame
        test_data = [Row(id="1", name="Test")]
        df = spark_session.createDataFrame(test_data)
        
        loader = S3Loader(spark_session, test_config)
        result = loader.load(df, "s3a://test-bucket/data/", format="parquet")
        
        assert result is not None

