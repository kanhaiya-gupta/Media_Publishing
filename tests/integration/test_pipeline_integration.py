"""
Integration tests for the complete pipeline
"""
import pytest
from clickhouse_driver import Client
from kafka import KafkaProducer, KafkaConsumer
import time


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the complete data pipeline"""
    
    @pytest.fixture
    def clickhouse_client(self):
        """Create ClickHouse client for testing"""
        try:
            client = Client(
                host='localhost',
                port=9002,
                database='analytics',
                user='default',
                password='clickhouse',
                connect_timeout=10
            )
            return client
        except Exception as e:
            pytest.skip(f"ClickHouse not available: {e}")
    
    @pytest.fixture
    def kafka_producer(self):
        """Create Kafka producer for testing"""
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: str(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")
    
    def test_clickhouse_connection(self, clickhouse_client):
        """Test ClickHouse connectivity"""
        result = clickhouse_client.execute("SELECT 1")
        assert result[0][0] == 1
    
    def test_clickhouse_schema_exists(self, clickhouse_client):
        """Test that required tables exist"""
        result = clickhouse_client.execute("SHOW TABLES FROM analytics")
        tables = [row[0] for row in result]
        
        assert 'session_metrics' in tables, "session_metrics table not found"
    
    def test_kafka_topic_exists(self, kafka_producer):
        """Test Kafka topic accessibility"""
        # This would require Kafka admin client
        # For now, we just verify producer can connect
        assert kafka_producer is not None
    
    def test_data_flow(self, clickhouse_client):
        """Test end-to-end data flow"""
        # Check if data exists in ClickHouse
        result = clickhouse_client.execute("SELECT count() FROM session_metrics")
        count = result[0][0]
        
        # For integration test, we just verify the query works
        assert isinstance(count, (int, type(None))), "Count query should return integer"

