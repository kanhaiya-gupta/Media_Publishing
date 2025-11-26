"""
Airflow Configuration
====================

Configuration management for Airflow DAGs.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class AirflowConfig:
    """
    Airflow configuration for DAGs.
    
    Contains configuration for DAG execution and ETL pipelines.
    """
    
    # Airflow settings
    default_owner: str = field(default_factory=lambda: os.getenv("AIRFLOW_OWNER", "ownlens"))
    default_email: str = field(default_factory=lambda: os.getenv("AIRFLOW_EMAIL", "admin@ownlens.com"))
    default_retries: int = field(default_factory=lambda: int(os.getenv("AIRFLOW_RETRIES", "3")))
    default_retry_delay_minutes: int = field(default_factory=lambda: int(os.getenv("AIRFLOW_RETRY_DELAY", "5")))
    default_execution_timeout_hours: int = field(default_factory=lambda: int(os.getenv("AIRFLOW_EXECUTION_TIMEOUT", "2")))
    
    # ETL settings
    spark_master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    spark_app_name: str = "OwnLensETL"
    
    # Additional configuration
    extra_config: Dict[str, Any] = field(default_factory=dict)
    
    def get_default_args(self) -> Dict[str, Any]:
        """Get default arguments for DAGs."""
        from datetime import timedelta
        
        return {
            "owner": self.default_owner,
            "depends_on_past": False,
            "email": [self.default_email],
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": self.default_retries,
            "retry_delay": timedelta(minutes=self.default_retry_delay_minutes),
            "execution_timeout": timedelta(hours=self.default_execution_timeout_hours),
        }
    
    def update(self, **kwargs):
        """Update configuration with new values."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.extra_config[key] = value


# Global configuration instance
_airflow_config: Optional[AirflowConfig] = None


def get_airflow_config() -> AirflowConfig:
    """
    Get global Airflow configuration instance.
    
    Returns:
        AirflowConfig instance
    """
    global _airflow_config
    if _airflow_config is None:
        _airflow_config = AirflowConfig()
    return _airflow_config


def set_airflow_config(config: AirflowConfig) -> None:
    """
    Set global Airflow configuration instance.
    
    Args:
        config: AirflowConfig instance
    """
    global _airflow_config
    _airflow_config = config


