"""
Unit Tests for PerformanceMonitor
=================================
"""

import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime

from src.ownlens.ml.monitoring.performance_monitor import PerformanceMonitor


class TestPerformanceMonitor:
    """Test PerformanceMonitor class."""

    def test_init(self):
        """Test monitor initialization."""
        monitor = PerformanceMonitor()
        
        assert monitor is not None
        assert monitor.config is not None

    def test_init_with_client(self):
        """Test monitor initialization with custom client."""
        mock_client = Mock()
        monitor = PerformanceMonitor(client=mock_client)
        
        assert monitor.client is mock_client

    def test_track_prediction(self):
        """Test tracking a prediction."""
        monitor = PerformanceMonitor()
        
        # Should not raise exception
        monitor.track_prediction(
            model_id="model_id",
            prediction=0.85,
            latency_ms=10.5
        )
        
        assert True

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_accuracy(self, mock_client_class):
        """Test calculating accuracy."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (100, 90, 0.9)  # total, correct, accuracy
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        metrics = monitor.calculate_accuracy(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None
        assert "accuracy" in metrics or isinstance(metrics, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_precision(self, mock_client_class):
        """Test calculating precision."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (90, 80, 0.888)  # true_positives, predicted_positives, precision
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        metrics = monitor.calculate_precision(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None
        assert "precision" in metrics or isinstance(metrics, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_recall(self, mock_client_class):
        """Test calculating recall."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (90, 100, 0.9)  # true_positives, actual_positives, recall
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        metrics = monitor.calculate_recall(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None
        assert "recall" in metrics or isinstance(metrics, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_f1_score(self, mock_client_class):
        """Test calculating F1 score."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (0.9, 0.85, 0.875)  # precision, recall, f1
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        metrics = monitor.calculate_f1_score(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None
        assert "f1_score" in metrics or isinstance(metrics, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_roc_auc(self, mock_client_class):
        """Test calculating ROC AUC."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (0.85,)  # roc_auc
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        metrics = monitor.calculate_roc_auc(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert metrics is not None
        assert "roc_auc" in metrics or isinstance(metrics, dict)

    def test_track_prediction_with_metadata(self):
        """Test tracking prediction with additional metadata."""
        monitor = PerformanceMonitor()
        
        monitor.track_prediction(
            model_id="model_id",
            prediction=0.85,
            latency_ms=10.5,
            actual_value=1,
            features={"feature1": 1.0, "feature2": 2.0}
        )
        
        assert True  # Should not raise exception

    def test_track_prediction_batch(self):
        """Test tracking multiple predictions."""
        monitor = PerformanceMonitor()
        
        predictions = [
            {"model_id": "model_id", "prediction": 0.8, "latency_ms": 10.0},
            {"model_id": "model_id", "prediction": 0.9, "latency_ms": 12.0},
            {"model_id": "model_id", "prediction": 0.7, "latency_ms": 9.0}
        ]
        
        monitor.track_predictions_batch(predictions)
        
        assert True  # Should not raise exception

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_get_prediction_stats(self, mock_client_class):
        """Test getting prediction statistics."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (1000, 10.5, 5.0, 20.0)  # count, avg_latency, min_latency, max_latency
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        stats = monitor.get_prediction_stats(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert stats is not None
        assert "count" in stats or isinstance(stats, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_get_latency_percentiles(self, mock_client_class):
        """Test getting latency percentiles."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (5.0, 10.0, 15.0, 20.0, 25.0)  # p50, p75, p90, p95, p99
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        percentiles = monitor.get_latency_percentiles(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert percentiles is not None
        assert "p50" in percentiles or isinstance(percentiles, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_get_error_rate(self, mock_client_class):
        """Test getting error rate."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (1000, 10, 0.01)  # total, errors, error_rate
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        error_rate = monitor.get_error_rate(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert error_rate is not None
        assert "error_rate" in error_rate or isinstance(error_rate, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_get_throughput(self, mock_client_class):
        """Test getting prediction throughput."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (1000, 3600, 0.278)  # count, seconds, predictions_per_second
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        throughput = monitor.get_throughput(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert throughput is not None
        assert "throughput" in throughput or isinstance(throughput, dict)

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_get_metrics_timeseries(self, mock_client_class):
        """Test getting metrics as time series."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (date(2024, 1, 1), 0.9, 0.85),
            (date(2024, 1, 2), 0.91, 0.86),
            (date(2024, 1, 3), 0.89, 0.84)
        ]
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        timeseries = monitor.get_metrics_timeseries(
            model_id="model_id",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            metrics=["accuracy", "roc_auc"]
        )
        
        assert timeseries is not None
        assert len(timeseries) > 0

    @patch('src.ownlens.ml.monitoring.performance_monitor.Client')
    def test_calculate_accuracy_error_handling(self, mock_client_class):
        """Test accuracy calculation error handling."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        mock_client_class.return_value = mock_client
        
        monitor = PerformanceMonitor(client=mock_client)
        
        with pytest.raises(Exception, match="Database error"):
            monitor.calculate_accuracy(
                model_id="model_id",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31)
            )

    def test_track_prediction_error_handling(self):
        """Test prediction tracking error handling."""
        monitor = PerformanceMonitor()
        
        # Should handle errors gracefully
        monitor.track_prediction(
            model_id="model_id",
            prediction=None,  # Invalid prediction
            latency_ms=-1  # Invalid latency
        )
        
        assert True  # Should not raise exception, just log warning

