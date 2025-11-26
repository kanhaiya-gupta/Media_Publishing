"""
End-to-End Tests for ETL Pipeline
==================================
"""

import pytest
from pyspark.sql import SparkSession

from src.ownlens.processing.etl.orchestration import run_etl_pipeline


@pytest.mark.e2e
class TestETLPipelineE2E:
    """End-to-end tests for ETL pipeline."""

    def test_complete_etl_pipeline(self, spark_session):
        """Test complete ETL pipeline execution."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        results = run_etl_pipeline(
            source="postgresql",
            load=True,
            load_destinations=["postgresql", "clickhouse"]
        )
        
        assert results is not None
        assert "postgresql" in results

    def test_etl_pipeline_all_sources(self, spark_session):
        """Test ETL pipeline with all sources."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        results = run_etl_pipeline(
            source=None,  # All sources
            load=True,
            load_destinations=["postgresql", "clickhouse", "s3"]
        )
        
        assert results is not None

    def test_etl_pipeline_error_recovery(self, spark_session):
        """Test ETL pipeline error recovery."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        # Test with invalid table to verify error handling
        results = run_etl_pipeline(
            source="postgresql",
            tables=["non_existent_table"],
            load=False
        )
        
        assert results is not None

