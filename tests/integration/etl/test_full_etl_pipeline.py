"""
Integration Tests for Full ETL Pipeline
========================================
"""

import pytest


@pytest.mark.integration
class TestFullETLPipeline:
    """Integration tests for full ETL pipeline."""

    def test_full_etl_pipeline_postgresql_to_clickhouse(self, spark_session, test_config):
        """Test full ETL pipeline from PostgreSQL to ClickHouse."""
        # Skip if test environment is not available
        pytest.skip("Requires test PostgreSQL and ClickHouse databases")
        
        from src.ownlens.processing.etl.orchestration import run_etl_pipeline
        
        results = run_etl_pipeline(
            source="postgresql",
            load=True,
            load_destinations=["clickhouse"],
            tables=["countries", "cities"]
        )
        
        assert results is not None
        assert "postgresql" in results

    def test_full_etl_pipeline_all_sources(self, spark_session, test_config):
        """Test full ETL pipeline from all sources."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment")
        
        from src.ownlens.processing.etl.orchestration import run_etl_pipeline
        
        results = run_etl_pipeline(
            source=None,  # All sources
            load=True,
            load_destinations=["postgresql", "clickhouse", "s3"]
        )
        
        assert results is not None

