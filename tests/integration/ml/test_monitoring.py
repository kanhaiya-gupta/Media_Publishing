"""
Integration Tests for ML Monitoring
====================================
"""

import pytest


@pytest.mark.integration
@pytest.mark.ml
class TestMLMonitoring:
    """Integration tests for ML monitoring."""

    def test_calculate_accuracy_integration(self):
        """Test calculating accuracy from predictions."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database with predictions")
        
        from src.ownlens.ml.monitoring.performance_monitor import PerformanceMonitor
        from datetime import date
        
        monitor = PerformanceMonitor()
        
        metrics = monitor.calculate_accuracy(
            model_id="test_model",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None

    def test_drift_detection_integration(self):
        """Test drift detection with test data."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database with data")
        
        from src.ownlens.ml.monitoring.drift_detector import DriftDetector
        import pandas as pd
        import numpy as np
        
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="test_model",
            current_features=current_features,
            training_features=training_features
        )
        
        assert result is not None

