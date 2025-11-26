"""
OwnLens - ML Module: Configuration

Configuration management for ML scripts following ownlens framework patterns.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class MLConfig:
    """
    Configuration for ML scripts.
    
    Uses environment variables with sensible defaults, following ownlens framework patterns.
    """
    
    # ClickHouse configuration
    clickhouse_host: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "9002")))
    clickhouse_database: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_DB", "ownlens_analytics"))
    clickhouse_user: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    
    # ML-specific settings
    ml_models_dir: str = field(default_factory=lambda: os.getenv("ML_MODELS_DIR", "models"))
    ml_data_dir: str = field(default_factory=lambda: os.getenv("ML_DATA_DIR", "."))
    ml_results_dir: str = field(default_factory=lambda: os.getenv("ML_RESULTS_DIR", "."))
    
    # Data loading settings
    default_limit: int = field(default_factory=lambda: int(os.getenv("ML_DEFAULT_LIMIT", "10000")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("ML_BATCH_SIZE", "10000")))
    
    # Connection timeouts
    connect_timeout: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT", "10")))
    send_receive_timeout: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "300")))
    
    # Additional configuration
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    def get_clickhouse_connection_params(self) -> Dict[str, Any]:
        """Get ClickHouse connection parameters as a dictionary."""
        return {
            "host": self.clickhouse_host,
            "port": self.clickhouse_port,
            "database": self.clickhouse_database,
            "user": self.clickhouse_user,
            "password": self.clickhouse_password,
            "connect_timeout": self.connect_timeout,
            "send_receive_timeout": self.send_receive_timeout,
        }


# Global configuration instance
_ml_config: MLConfig = None


def get_ml_config() -> MLConfig:
    """Get the global ML configuration instance."""
    global _ml_config
    if _ml_config is None:
        _ml_config = MLConfig()
    return _ml_config


def set_ml_config(config: MLConfig):
    """Set the global ML configuration instance."""
    global _ml_config
    _ml_config = config

