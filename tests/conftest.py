"""
Pytest Configuration and Fixtures
===================================

Common fixtures for all tests.
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession

# Import configuration
from src.ownlens.processing.etl.utils.config import ETLConfig, get_etl_config, set_etl_config
from src.ownlens.processing.etl.utils.spark_session import SparkSessionManager


@pytest.fixture(scope="session")
def test_config() -> ETLConfig:
    """Create test ETL configuration."""
    config = ETLConfig(
        spark_master="local[2]",  # Use 2 cores for testing
        spark_app_name="OwnLensETLTest",
        postgresql_host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
        postgresql_port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
        postgresql_database=os.getenv("TEST_POSTGRES_DB", "ownlens_test"),
        postgresql_user=os.getenv("TEST_POSTGRES_USER", "postgres"),
        postgresql_password=os.getenv("TEST_POSTGRES_PASSWORD", "postgres"),
        postgresql_raw_database=os.getenv("TEST_POSTGRES_RAW_DB", "ownlens_test_raw"),
        kafka_bootstrap_servers=os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        s3_endpoint=os.getenv("TEST_S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.getenv("TEST_S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.getenv("TEST_S3_SECRET_KEY", "minioadmin"),
        s3_bucket=os.getenv("TEST_S3_BUCKET", "ownlens-test"),
        clickhouse_host=os.getenv("TEST_CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("TEST_CLICKHOUSE_PORT", "9000")),
        clickhouse_database=os.getenv("TEST_CLICKHOUSE_DB", "ownlens_analytics_test"),
        clickhouse_user=os.getenv("TEST_CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("TEST_CLICKHOUSE_PASSWORD", ""),
    )
    return config


@pytest.fixture(scope="function")
def spark_session(test_config: ETLConfig) -> SparkSession:
    """Create a test Spark session."""
    # Create a minimal Spark session for testing
    spark = SparkSessionManager.create_session(
        app_name="test",
        master="local[2]",
        config={
            "spark.sql.shuffle.partitions": "2",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.sql.adaptive.enabled": "true",
        },
        session_type="test"
    )
    
    yield spark
    
    # Cleanup
    SparkSessionManager.stop_session(session_type="test", app_name="test")


@pytest.fixture(scope="function")
def mock_spark_session() -> Mock:
    """Create a mock Spark session for unit tests."""
    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sql = Mock(return_value=Mock())
    spark.createDataFrame = Mock()
    spark.read = Mock()
    return spark


@pytest.fixture(scope="function")
def temp_dir() -> Path:
    """Create a temporary directory for test files."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def sample_data_dir(temp_dir: Path) -> Path:
    """Create a directory for sample test data."""
    data_dir = temp_dir / "sample_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture(scope="function")
def mock_postgresql_connection():
    """Create a mock PostgreSQL connection."""
    connection = Mock()
    connection.cursor = Mock(return_value=Mock())
    connection.close = Mock()
    return connection


@pytest.fixture(scope="function")
def mock_kafka_consumer():
    """Create a mock Kafka consumer."""
    consumer = Mock()
    consumer.poll = Mock(return_value={})
    consumer.close = Mock()
    return consumer


@pytest.fixture(scope="function")
def mock_s3_client():
    """Create a mock S3 client."""
    client = Mock()
    client.list_objects = Mock(return_value={"Contents": []})
    client.get_object = Mock(return_value={"Body": Mock()})
    client.put_object = Mock()
    return client


@pytest.fixture(scope="function")
def mock_clickhouse_client():
    """Create a mock ClickHouse client."""
    client = Mock()
    client.execute = Mock(return_value=[])
    client.insert = Mock()
    return client


@pytest.fixture(scope="function")
def sample_countries_data():
    """Sample countries data for testing."""
    return [
        {"id": "1", "name": "United States", "code": "US", "region": "North America"},
        {"id": "2", "name": "United Kingdom", "code": "GB", "region": "Europe"},
        {"id": "3", "name": "Canada", "code": "CA", "region": "North America"},
    ]


@pytest.fixture(scope="function")
def sample_cities_data():
    """Sample cities data for testing."""
    return [
        {"id": "1", "name": "New York", "country_id": "1", "population": 8000000},
        {"id": "2", "name": "London", "country_id": "2", "population": 9000000},
        {"id": "3", "name": "Toronto", "country_id": "3", "population": 3000000},
    ]


@pytest.fixture(scope="function")
def sample_users_data():
    """Sample users data for testing."""
    return [
        {
            "id": "1",
            "email": "user1@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "country_id": "1",
        },
        {
            "id": "2",
            "email": "user2@example.com",
            "first_name": "Jane",
            "last_name": "Smith",
            "country_id": "2",
        },
    ]


@pytest.fixture(scope="function", autouse=True)
def reset_etl_config():
    """Reset ETL config before each test."""
    # Save original config
    original_config = get_etl_config()
    yield
    # Restore original config
    set_etl_config(original_config)

