"""
Unit Tests for DriftDetector
============================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np

from src.ownlens.ml.monitoring.drift_detector import DriftDetector


class TestDriftDetector:
    """Test DriftDetector class."""

    def test_init(self):
        """Test detector initialization."""
        detector = DriftDetector()
        
        assert detector is not None
        assert detector.config is not None

    def test_init_with_client(self):
        """Test detector initialization with custom client."""
        mock_client = Mock()
        detector = DriftDetector(client=mock_client)
        
        assert detector.client is mock_client

    def test_detect_data_drift(self):
        """Test data drift detection."""
        detector = DriftDetector()
        
        # Create sample data
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1
        )
        
        assert result is not None
        assert "drift_detected" in result
        assert "drift_scores" in result

    def test_detect_performance_drift(self):
        """Test performance drift detection."""
        detector = DriftDetector()
        
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_accuracy=0.75,
            baseline_accuracy=0.85,
            threshold=0.05
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_data_drift_with_statistical_test(self):
        """Test data drift detection with statistical test."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0.5, 1, 100),  # Shifted distribution
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1,
            test_type="ks_test"  # Kolmogorov-Smirnov test
        )
        
        assert result is not None
        assert "drift_detected" in result
        assert "drift_scores" in result

    def test_detect_data_drift_with_psi(self):
        """Test data drift detection with PSI (Population Stability Index)."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1,
            test_type="psi"
        )
        
        assert result is not None
        assert "drift_detected" in result
        assert "drift_scores" in result

    def test_detect_data_drift_with_mmd(self):
        """Test data drift detection with MMD (Maximum Mean Discrepancy)."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1,
            test_type="mmd"
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_performance_drift_no_drift(self):
        """Test performance drift detection when no drift exists."""
        detector = DriftDetector()
        
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_accuracy=0.88,
            baseline_accuracy=0.85,
            threshold=0.05
        )
        
        assert result is not None
        assert result.get("drift_detected", False) is False

    def test_detect_performance_drift_with_roc_auc(self):
        """Test performance drift detection with ROC AUC."""
        detector = DriftDetector()
        
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_roc_auc=0.75,
            baseline_roc_auc=0.85,
            threshold=0.05,
            metric="roc_auc"
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_performance_drift_with_precision(self):
        """Test performance drift detection with precision."""
        detector = DriftDetector()
        
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_precision=0.70,
            baseline_precision=0.80,
            threshold=0.05,
            metric="precision"
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_performance_drift_with_recall(self):
        """Test performance drift detection with recall."""
        detector = DriftDetector()
        
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_recall=0.65,
            baseline_recall=0.75,
            threshold=0.05,
            metric="recall"
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_data_drift_with_categorical_features(self):
        """Test data drift detection with categorical features."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": ["A", "B", "C"] * 33 + ["A"],
            "feature2": [1, 2, 3] * 33 + [1],
        })
        
        current_features = pd.DataFrame({
            "feature1": ["A", "B", "D"] * 33 + ["A"],  # New category
            "feature2": [1, 2, 3] * 33 + [1],
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_data_drift_with_missing_features(self):
        """Test data drift detection when features are missing."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            # Missing feature2
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1
        )
        
        # Should handle gracefully
        assert result is not None

    def test_detect_data_drift_with_different_sizes(self):
        """Test data drift detection with different sample sizes."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 1000),
            "feature2": np.random.normal(0, 1, 1000),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1
        )
        
        assert result is not None
        assert "drift_detected" in result

    @patch('src.ownlens.ml.monitoring.drift_detector.Client')
    def test_save_drift_report(self, mock_client_class):
        """Test saving drift detection report."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        detector = DriftDetector(client=mock_client)
        
        drift_result = {
            "drift_detected": True,
            "drift_scores": {"feature1": 0.15, "feature2": 0.05}
        }
        
        result = detector.save_drift_report(
            model_id="model_id",
            drift_result=drift_result
        )
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.monitoring.drift_detector.Client')
    def test_get_drift_history(self, mock_client_class):
        """Test getting drift detection history."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("2024-01-01", True, 0.15),
            ("2024-01-02", False, 0.05),
            ("2024-01-03", True, 0.20)
        ]
        mock_client_class.return_value = mock_client
        
        detector = DriftDetector(client=mock_client)
        
        history = detector.get_drift_history(
            model_id="model_id",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert history is not None
        assert len(history) > 0

    def test_detect_data_drift_error_handling(self):
        """Test data drift detection error handling."""
        detector = DriftDetector()
        
        # Invalid data
        training_features = pd.DataFrame()
        current_features = pd.DataFrame()
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1
        )
        
        # Should handle gracefully
        assert result is not None

    def test_detect_performance_drift_error_handling(self):
        """Test performance drift detection error handling."""
        detector = DriftDetector()
        
        # Invalid values
        result = detector.detect_performance_drift(
            model_id="model_id",
            current_accuracy=None,
            baseline_accuracy=0.85,
            threshold=0.05
        )
        
        # Should handle gracefully
        assert result is not None

    def test_detect_data_drift_with_feature_importance(self):
        """Test data drift detection with feature importance."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0.5, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        feature_importance = {"feature1": 0.8, "feature2": 0.2}
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1,
            feature_importance=feature_importance
        )
        
        assert result is not None
        assert "drift_detected" in result

    def test_detect_data_drift_with_percentile_threshold(self):
        """Test data drift detection with percentile-based threshold."""
        detector = DriftDetector()
        
        training_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        current_features = pd.DataFrame({
            "feature1": np.random.normal(0, 1, 100),
            "feature2": np.random.normal(0, 1, 100),
        })
        
        result = detector.detect_data_drift(
            model_id="model_id",
            current_features=current_features,
            training_features=training_features,
            threshold=0.1,
            use_percentile=True,
            percentile=95
        )
        
        assert result is not None
        assert "drift_detected" in result

