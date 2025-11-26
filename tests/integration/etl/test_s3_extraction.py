"""
Integration Tests for S3 Extraction
===================================
"""

import pytest


@pytest.mark.integration
class TestS3Extraction:
    """Integration tests for S3 extraction."""

    def test_extract_s3_paths_integration(self, spark_session, test_config):
        """Test S3 path extraction with real S3/MinIO (if available)."""
        # Skip if test S3 is not available
        pytest.skip("Requires test S3/MinIO bucket")
        
        from src.ownlens.processing.etl.extractors.s3_extractor import S3Extractor
        
        extractor = S3Extractor(spark_session, test_config)
        df = extractor.extract(path="data/", format="parquet")
        
        assert df is not None

