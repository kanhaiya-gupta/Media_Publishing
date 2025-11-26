"""
Integration Tests for Kafka Extraction
======================================
"""

import pytest


@pytest.mark.integration
class TestKafkaExtraction:
    """Integration tests for Kafka extraction."""

    def test_extract_kafka_topics_integration(self, spark_session, test_config):
        """Test Kafka topic extraction with real Kafka (if available)."""
        # Skip if test Kafka is not available
        pytest.skip("Requires test Kafka cluster")
        
        from src.ownlens.processing.etl.extractors.kafka_extractor import KafkaExtractor
        
        extractor = KafkaExtractor(spark_session, test_config)
        df = extractor.extract(topics="customer-user-events")
        
        assert df is not None

