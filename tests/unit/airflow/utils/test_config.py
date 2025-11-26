"""
Unit Tests for AirflowConfig
=============================
"""

import pytest
from unittest.mock import patch
import os

from src.ownlens.processing.airflow.plugins.utils.config import (
    AirflowConfig,
    get_airflow_config,
    set_airflow_config,
)


class TestAirflowConfig:
    """Test AirflowConfig class."""

    def test_init(self):
        """Test config initialization."""
        config = AirflowConfig()
        
        assert config is not None
        assert config.default_owner == "ownlens"
        assert config.default_email == "admin@ownlens.com"
        assert config.default_retries == 3

    @patch.dict(os.environ, {
        "AIRFLOW_OWNER": "test_owner",
        "AIRFLOW_EMAIL": "test@example.com",
        "AIRFLOW_RETRIES": "5",
    })
    def test_init_from_env(self):
        """Test config initialization from environment variables."""
        config = AirflowConfig()
        
        assert config.default_owner == "test_owner"
        assert config.default_email == "test@example.com"
        assert config.default_retries == 5

    def test_get_default_args(self):
        """Test getting default arguments."""
        config = AirflowConfig()
        
        default_args = config.get_default_args()
        
        assert "owner" in default_args
        assert "retries" in default_args
        assert "retry_delay" in default_args
        assert "execution_timeout" in default_args

    def test_update(self):
        """Test updating configuration."""
        config = AirflowConfig()
        
        config.update(
            default_owner="new_owner",
            default_retries=10
        )
        
        assert config.default_owner == "new_owner"
        assert config.default_retries == 10

    def test_update_extra_config(self):
        """Test updating extra configuration."""
        config = AirflowConfig()
        
        config.update(custom_key="custom_value")
        
        assert config.extra_config["custom_key"] == "custom_value"


class TestGetAirflowConfig:
    """Test get_airflow_config function."""

    def test_get_airflow_config_singleton(self):
        """Test that get_airflow_config returns singleton."""
        config1 = get_airflow_config()
        config2 = get_airflow_config()
        
        assert config1 is config2

    def test_set_airflow_config(self):
        """Test setting Airflow config."""
        original_config = get_airflow_config()
        
        new_config = AirflowConfig(default_owner="new_owner")
        set_airflow_config(new_config)
        
        current_config = get_airflow_config()
        assert current_config is new_config
        assert current_config.default_owner == "new_owner"
        
        # Restore original
        set_airflow_config(original_config)

