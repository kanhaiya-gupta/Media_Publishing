"""
Unit Tests for ETL Configuration
=================================
"""

import pytest
import os
from unittest.mock import patch

from src.ownlens.processing.etl.utils.config import (
    ETLConfig,
    get_etl_config,
    set_etl_config,
)


class TestETLConfig:
    """Test ETLConfig class."""

    def test_etl_config_defaults(self):
        """Test ETL config with default values."""
        config = ETLConfig()
        
        assert config.spark_master == "local[*]"
        assert config.spark_app_name == "OwnLensETL"
        assert config.postgresql_host == "localhost"
        assert config.postgresql_port == 5432

    def test_etl_config_custom_values(self):
        """Test ETL config with custom values."""
        config = ETLConfig(
            spark_master="local[4]",
            postgresql_host="test_host",
            postgresql_port=5433,
            postgresql_database="test_db",
        )
        
        assert config.spark_master == "local[4]"
        assert config.postgresql_host == "test_host"
        assert config.postgresql_port == 5433
        assert config.postgresql_database == "test_db"

    @patch.dict(os.environ, {
        "POSTGRES_HOST": "env_host",
        "POSTGRES_PORT": "5434",
        "POSTGRES_DB": "env_db",
    })
    def test_etl_config_from_env(self):
        """Test ETL config from environment variables."""
        config = ETLConfig()
        
        assert config.postgresql_host == "env_host"
        assert config.postgresql_port == 5434
        assert config.postgresql_database == "env_db"

    def test_get_postgresql_url_production(self):
        """Test PostgreSQL URL for production database."""
        config = ETLConfig(
            postgresql_host="localhost",
            postgresql_port=5432,
            postgresql_database="ownlens",
        )
        
        url = config.get_postgresql_url(use_raw=False)
        assert "ownlens" in url
        assert "localhost" in url
        assert "5432" in url
        assert "stringtype=unspecified" in url

    def test_get_postgresql_url_raw(self):
        """Test PostgreSQL URL for raw database."""
        config = ETLConfig(
            postgresql_host="localhost",
            postgresql_port=5432,
            postgresql_raw_database="ownlens_raw",
        )
        
        url = config.get_postgresql_url(use_raw=True)
        assert "ownlens_raw" in url
        assert "stringtype=unspecified" in url

    def test_get_postgresql_properties(self):
        """Test PostgreSQL connection properties."""
        config = ETLConfig(
            postgresql_user="test_user",
            postgresql_password="test_password",
        )
        
        props = config.get_postgresql_properties()
        
        assert props["user"] == "test_user"
        assert props["password"] == "test_password"
        assert props["driver"] == "org.postgresql.Driver"

    def test_get_s3_config(self):
        """Test S3 configuration."""
        config = ETLConfig(
            s3_endpoint="http://localhost:9000",
            s3_access_key="test_key",
            s3_secret_key="test_secret",
        )
        
        s3_config = config.get_s3_config()
        
        assert s3_config["fs.s3a.endpoint"] == "http://localhost:9000"
        assert s3_config["fs.s3a.access.key"] == "test_key"
        assert s3_config["fs.s3a.secret.key"] == "test_secret"

    def test_get_kafka_config(self):
        """Test Kafka configuration."""
        config = ETLConfig(
            kafka_bootstrap_servers="localhost:9092",
            kafka_security_protocol="PLAINTEXT",
        )
        
        kafka_config = config.get_kafka_config()
        
        assert kafka_config["kafka.bootstrap.servers"] == "localhost:9092"
        assert kafka_config["kafka.security.protocol"] == "PLAINTEXT"

    def test_update_config(self):
        """Test updating configuration."""
        config = ETLConfig()
        
        config.update(
            postgresql_host="updated_host",
            postgresql_port=5435,
        )
        
        assert config.postgresql_host == "updated_host"
        assert config.postgresql_port == 5435

    def test_update_config_extra(self):
        """Test updating configuration with extra keys."""
        config = ETLConfig()
        
        config.update(custom_key="custom_value")
        
        assert config.extra_config["custom_key"] == "custom_value"


class TestGetETLConfig:
    """Test get_etl_config function."""

    def test_get_etl_config_singleton(self):
        """Test that get_etl_config returns singleton."""
        config1 = get_etl_config()
        config2 = get_etl_config()
        
        assert config1 is config2

    def test_set_etl_config(self):
        """Test setting ETL config."""
        original_config = get_etl_config()
        
        new_config = ETLConfig(postgresql_host="new_host")
        set_etl_config(new_config)
        
        current_config = get_etl_config()
        assert current_config is new_config
        assert current_config.postgresql_host == "new_host"
        
        # Restore original
        set_etl_config(original_config)

