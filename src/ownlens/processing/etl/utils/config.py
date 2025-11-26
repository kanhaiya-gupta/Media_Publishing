"""
ETL Configuration
=================

Configuration management for ETL pipelines.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
import logging

# Load environment variables from development.env if available
try:
    from dotenv import load_dotenv
    # Try to find development.env at project root
    project_root = Path(__file__).parent.parent.parent.parent.parent
    env_file = project_root / "development.env"
    if env_file.exists():
        load_dotenv(env_file)
        logger = logging.getLogger(__name__)
        logger.info(f"Loaded ETL environment variables from {env_file}")
    else:
        logger = logging.getLogger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    # python-dotenv not available, use system environment variables only


@dataclass
class ETLConfig:
    """
    ETL pipeline configuration.
    
    Contains configuration for extractors, transformers, and loaders.
    """
    
    # Spark configuration
    spark_master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    spark_app_name: str = "OwnLensETL"
    
    # PostgreSQL configuration (production database)
    postgresql_host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    postgresql_port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    postgresql_database: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "ownlens"))
    postgresql_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    postgresql_password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", ""))
    
    # Raw database for data generation (separate from production)
    # If not set, defaults to {POSTGRES_DB}_raw (e.g., "ownlens_raw")
    postgresql_raw_database: str = field(default_factory=lambda: os.getenv("POSTGRES_RAW_DB") or (os.getenv("POSTGRES_DB", "ownlens") + "_raw"))
    
    # Kafka configuration
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    kafka_security_protocol: str = field(default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    
    # S3/MinIO configuration
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", "minioadmin"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", "ownlens"))
    
    # ClickHouse configuration (optional)
    clickhouse_host: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "9000")))
    clickhouse_database: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_DB", "ownlens_analytics"))
    clickhouse_user: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    
    # ETL settings
    batch_size: int = field(default_factory=lambda: int(os.getenv("ETL_BATCH_SIZE", "10000")))
    checkpoint_location: str = field(default_factory=lambda: os.getenv("ETL_CHECKPOINT_LOCATION", "/tmp/spark-checkpoint"))
    
    # Additional configuration
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    def get_postgresql_url(self, use_raw: bool = False) -> str:
        """
        Get PostgreSQL JDBC URL.
        
        Args:
            use_raw: If True, use raw database (for extraction). If False, use production database (for loading).
        
        Returns:
            JDBC URL string
        """
        # Add ?stringtype=unspecified to allow PostgreSQL to auto-convert:
        # - StringType UUID strings → UUID type
        # - StringType ISO timestamp strings → timestamp with time zone
        database = self.postgresql_raw_database if use_raw else self.postgresql_database
        return f"jdbc:postgresql://{self.postgresql_host}:{self.postgresql_port}/{database}?stringtype=unspecified"
    
    def get_postgresql_properties(self) -> Dict[str, str]:
        """Get PostgreSQL connection properties."""
        return {
            "user": self.postgresql_user,
            "password": self.postgresql_password,
            "driver": "org.postgresql.Driver"
            # Note: stringtype=unspecified is now in the JDBC URL, not here
        }
    
    def get_s3_config(self) -> Dict[str, str]:
        """Get S3/MinIO configuration for Spark."""
        return {
            "fs.s3a.endpoint": self.s3_endpoint,
            "fs.s3a.access.key": self.s3_access_key,
            "fs.s3a.secret.key": self.s3_secret_key,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    
    def get_kafka_config(self) -> Dict[str, str]:
        """Get Kafka configuration for Spark."""
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "kafka.security.protocol": self.kafka_security_protocol
        }
    
    def update(self, **kwargs):
        """Update configuration with new values."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.extra_config[key] = value


# Global configuration instance
_etl_config: Optional[ETLConfig] = None


def get_etl_config() -> ETLConfig:
    """
    Get or create global ETL configuration.
    
    Returns:
        ETLConfig instance
    """
    global _etl_config
    if _etl_config is None:
        _etl_config = ETLConfig()
        logger.info("ETL configuration initialized")
    return _etl_config


def set_etl_config(config: ETLConfig):
    """
    Set global ETL configuration.
    
    Args:
        config: ETLConfig instance
    """
    global _etl_config
    _etl_config = config
    logger.info("ETL configuration updated")

