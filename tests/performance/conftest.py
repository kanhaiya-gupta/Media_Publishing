"""
Performance Test Fixtures
==========================
"""

import pytest
import time
import psutil
import os
from typing import Dict, Any, List
from unittest.mock import Mock
from pyspark.sql import SparkSession, DataFrame

from tests.conftest import mock_spark_session, test_config


@pytest.fixture
def performance_config():
    """Performance test configuration."""
    return {
        "load_test_iterations": 100,
        "stress_test_iterations": 1000,
        "memory_profiling": True,
        "throughput_measurement": True,
        "latency_measurement": True,
    }


@pytest.fixture
def performance_metrics():
    """Container for performance metrics."""
    return {
        "execution_times": [],
        "memory_usage": [],
        "throughput": [],
        "latency": [],
        "cpu_usage": [],
    }


class PerformanceMonitor:
    """Monitor performance metrics during tests."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.end_memory = None
        self.process = psutil.Process(os.getpid())
    
    def start(self):
        """Start monitoring."""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
    
    def stop(self):
        """Stop monitoring and return metrics."""
        self.end_time = time.time()
        self.end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            "execution_time": self.end_time - self.start_time,
            "memory_delta": self.end_memory - self.start_memory,
            "peak_memory": self.end_memory,
        }
    
    def get_current_metrics(self):
        """Get current performance metrics."""
        return {
            "memory_mb": self.process.memory_info().rss / 1024 / 1024,
            "cpu_percent": self.process.cpu_percent(),
        }


@pytest.fixture
def perf_monitor():
    """Performance monitor fixture."""
    return PerformanceMonitor()

