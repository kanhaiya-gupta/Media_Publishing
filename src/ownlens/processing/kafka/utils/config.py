"""
Kafka Configuration
===================

Configuration management for Kafka producers and consumers.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """
    Kafka configuration for producers and consumers.
    
    Contains configuration for Kafka connection and operations.
    """
    
    # Kafka connection
    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    security_protocol: str = field(default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    
    # Producer configuration
    compression_type: str = "snappy"
    acks: str = "all"  # Wait for all replicas
    idempotent: bool = True  # Exactly-once semantics
    retries: int = 3
    max_in_flight_requests_per_connection: int = 5
    
    # Consumer configuration
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    
    # Additional configuration
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "compression_type": self.compression_type,
            "acks": self.acks,
            "idempotent": self.idempotent,
            "retries": self.retries,
            "max_in_flight_requests_per_connection": self.max_in_flight_requests_per_connection,
            **self.extra_config
        }
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Get consumer configuration."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "auto_commit_interval_ms": self.auto_commit_interval_ms,
            **self.extra_config
        }
    
    def update(self, **kwargs):
        """Update configuration with new values."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.extra_config[key] = value


# Global configuration instance
_kafka_config: Optional[KafkaConfig] = None


def get_kafka_config() -> KafkaConfig:
    """
    Get global Kafka configuration instance.
    
    Returns:
        KafkaConfig instance
    """
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = KafkaConfig()
    return _kafka_config


def set_kafka_config(config: KafkaConfig) -> None:
    """
    Set global Kafka configuration instance.
    
    Args:
        config: KafkaConfig instance
    """
    global _kafka_config
    _kafka_config = config

