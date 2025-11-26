"""
Unit Tests for SparkSessionManager
===================================
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession

from src.ownlens.processing.etl.utils.spark_session import (
    SparkSessionManager,
    get_spark_session,
)


class TestSparkSessionManager:
    """Test SparkSessionManager class."""

    def test_create_session_success(self, test_config):
        """Test successful Spark session creation."""
        spark = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            session_type="test"
        )
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        assert spark.sparkContext.appName == "test_app"
        
        # Cleanup
        SparkSessionManager.stop_session(session_type="test", app_name="test_app")

    def test_create_session_reuse(self, test_config):
        """Test that existing session is reused."""
        spark1 = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            session_type="test"
        )
        
        spark2 = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            session_type="test"
        )
        
        assert spark1 is spark2  # Same session instance
        
        # Cleanup
        SparkSessionManager.stop_session(session_type="test", app_name="test_app")

    def test_create_session_with_config(self, test_config):
        """Test session creation with custom configuration."""
        config = {
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.memory": "1g",
        }
        
        spark = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            config=config,
            session_type="test"
        )
        
        assert spark is not None
        # Cleanup
        SparkSessionManager.stop_session(session_type="test", app_name="test_app")

    def test_create_streaming_session(self, test_config):
        """Test streaming session creation."""
        spark = SparkSessionManager.create_streaming_session(
            app_name="test_streaming",
            master="local[2]"
        )
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        # Cleanup
        SparkSessionManager.stop_session(session_type="streaming", app_name="test_streaming")

    def test_stop_session(self, test_config):
        """Test stopping a session."""
        spark = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            session_type="test"
        )
        
        assert spark is not None
        
        # Stop session
        SparkSessionManager.stop_session(session_type="test", app_name="test_app")
        
        # Verify session is removed
        session = SparkSessionManager.get_session("test_app", "test")
        assert session is None

    def test_stop_all_sessions(self, test_config):
        """Test stopping all sessions."""
        spark1 = SparkSessionManager.create_session(
            app_name="test_app1",
            master="local[2]",
            session_type="test"
        )
        spark2 = SparkSessionManager.create_session(
            app_name="test_app2",
            master="local[2]",
            session_type="test"
        )
        
        # Stop all sessions
        SparkSessionManager.stop_session()
        
        # Verify all sessions are removed
        assert SparkSessionManager.get_session("test_app1", "test") is None
        assert SparkSessionManager.get_session("test_app2", "test") is None

    def test_get_session_existing(self, test_config):
        """Test getting an existing session."""
        spark = SparkSessionManager.create_session(
            app_name="test_app",
            master="local[2]",
            session_type="test"
        )
        
        retrieved = SparkSessionManager.get_session("test_app", "test")
        assert retrieved is spark
        
        # Cleanup
        SparkSessionManager.stop_session(session_type="test", app_name="test_app")

    def test_get_session_nonexistent(self, test_config):
        """Test getting a non-existent session."""
        session = SparkSessionManager.get_session("nonexistent", "test")
        assert session is None


class TestGetSparkSession:
    """Test get_spark_session convenience function."""

    def test_get_spark_session_default(self, test_config):
        """Test default Spark session creation."""
        spark = get_spark_session(app_name="test_app")
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        # Cleanup
        SparkSessionManager.stop_session()

    def test_get_spark_session_streaming(self, test_config):
        """Test streaming Spark session creation."""
        spark = get_spark_session(
            app_name="test_streaming",
            streaming=True
        )
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        # Cleanup
        SparkSessionManager.stop_session(session_type="streaming", app_name="test_streaming")

    def test_get_spark_session_with_config(self, test_config):
        """Test Spark session creation with config."""
        config = {"spark.sql.shuffle.partitions": "2"}
        
        spark = get_spark_session(
            app_name="test_app",
            config=config
        )
        
        assert spark is not None
        
        # Cleanup
        SparkSessionManager.stop_session()

