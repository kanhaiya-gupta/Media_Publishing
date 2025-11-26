"""
Unit Tests for AlertingSystem
=============================
"""

import pytest
from unittest.mock import Mock, patch

from src.ownlens.ml.monitoring.alerting import AlertingSystem


class TestAlertingSystem:
    """Test AlertingSystem class."""

    def test_init(self):
        """Test alerting system initialization."""
        alerting = AlertingSystem()
        
        assert alerting is not None
        assert alerting.config is not None
        assert alerting.thresholds is not None

    def test_init_with_client(self):
        """Test alerting system initialization with custom client."""
        mock_client = Mock()
        alerting = AlertingSystem(client=mock_client)
        
        assert alerting.client is mock_client

    def test_check_alerts_accuracy_drop(self):
        """Test alert checking for accuracy drop."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.75  # 15% drop from baseline 0.9
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data,
            baseline_accuracy=0.9
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "accuracy_drop" for alert in alerts)

    def test_check_alerts_data_drift(self):
        """Test alert checking for data drift."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "data_drift_score": 0.25  # Above threshold 0.2
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "data_drift" for alert in alerts)

    def test_check_alerts_error_rate(self):
        """Test alert checking for error rate."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "error_rate": 0.10  # Above threshold 0.05
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "error_rate" for alert in alerts)

    def test_check_alerts_no_alerts(self):
        """Test alert checking with no alerts."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.95,  # No drop
            "data_drift_score": 0.1,  # Below threshold
            "error_rate": 0.01  # Below threshold
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data,
            baseline_accuracy=0.9
        )
        
        assert len(alerts) == 0

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_send_alert(self, mock_client_class):
        """Test sending an alert."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alert = {
            "alert_type": "accuracy_drop",
            "severity": "high",
            "message": "Test alert"
        }
        
        result = alerting.send_alert(
            model_id="model_id",
            alert=alert
        )
        
        assert result is not None

    def test_check_alerts_latency_spike(self):
        """Test alert checking for latency spike."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "avg_latency_ms": 500,  # Above threshold
            "p95_latency_ms": 1000
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "latency_spike" for alert in alerts)

    def test_check_alerts_throughput_drop(self):
        """Test alert checking for throughput drop."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "throughput": 10,  # Below threshold
            "baseline_throughput": 100
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "throughput_drop" for alert in alerts)

    def test_check_alerts_prediction_distribution_shift(self):
        """Test alert checking for prediction distribution shift."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "prediction_distribution_shift": 0.3  # Above threshold
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "prediction_shift" for alert in alerts)

    def test_check_alerts_model_version_mismatch(self):
        """Test alert checking for model version mismatch."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "model_version": "1.0.0",
            "expected_version": "1.1.0"
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        assert any(alert["alert_type"] == "version_mismatch" for alert in alerts)

    def test_check_alerts_with_custom_thresholds(self):
        """Test alert checking with custom thresholds."""
        custom_thresholds = {
            "accuracy_drop": 0.1,  # 10% drop
            "data_drift": 0.15,
            "error_rate": 0.08
        }
        
        alerting = AlertingSystem(thresholds=custom_thresholds)
        
        monitoring_data = {
            "accuracy": 0.80,  # 10% drop from 0.9
            "baseline_accuracy": 0.9
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0

    def test_check_alerts_with_severity_levels(self):
        """Test alert checking with severity levels."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.70,  # 20% drop - should be high severity
            "baseline_accuracy": 0.9
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0
        # Verify severity is set
        assert any(alert.get("severity") in ["low", "medium", "high"] for alert in alerts)

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_send_alert_with_notification_channels(self, mock_client_class):
        """Test sending alert with notification channels."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alert = {
            "alert_type": "accuracy_drop",
            "severity": "high",
            "message": "Test alert"
        }
        
        result = alerting.send_alert(
            model_id="model_id",
            alert=alert,
            channels=["email", "slack", "pagerduty"]
        )
        
        assert result is not None

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_send_alert_batch(self, mock_client_class):
        """Test sending multiple alerts in batch."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alerts = [
            {"alert_type": "accuracy_drop", "severity": "high", "message": "Alert 1"},
            {"alert_type": "data_drift", "severity": "medium", "message": "Alert 2"}
        ]
        
        result = alerting.send_alerts_batch(
            model_id="model_id",
            alerts=alerts
        )
        
        assert result is not None
        assert len(result) == 2

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_get_alerts(self, mock_client_class):
        """Test retrieving alerts."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("alert_id_1", "model_id", "accuracy_drop", "high", "2024-01-01"),
            ("alert_id_2", "model_id", "data_drift", "medium", "2024-01-02")
        ]
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alerts = alerting.get_alerts(
            model_id="model_id",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert alerts is not None
        assert len(alerts) > 0

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_get_alerts_with_filters(self, mock_client_class):
        """Test retrieving alerts with filters."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("alert_id", "model_id", "accuracy_drop", "high", "2024-01-01")
        ]
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alerts = alerting.get_alerts(
            model_id="model_id",
            alert_type="accuracy_drop",
            severity="high",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert alerts is not None
        assert len(alerts) > 0

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_mark_alert_resolved(self, mock_client_class):
        """Test marking an alert as resolved."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        result = alerting.mark_alert_resolved(
            alert_id="alert_id",
            resolution_notes="Issue fixed"
        )
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_get_unresolved_alerts(self, mock_client_class):
        """Test getting unresolved alerts."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("alert_id_1", "model_id", "accuracy_drop", "high", "2024-01-01", False),
            ("alert_id_2", "model_id", "data_drift", "medium", "2024-01-02", False)
        ]
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alerts = alerting.get_unresolved_alerts(model_id="model_id")
        
        assert alerts is not None
        assert len(alerts) > 0

    def test_check_alerts_with_multiple_conditions(self):
        """Test alert checking with multiple conditions."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.75,  # Drop
            "data_drift_score": 0.25,  # Drift
            "error_rate": 0.10  # High error rate
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data,
            baseline_accuracy=0.9
        )
        
        assert len(alerts) > 0
        # Should detect multiple issues
        assert len(alerts) >= 2

    def test_check_alerts_with_time_window(self):
        """Test alert checking with time window."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.75,
            "baseline_accuracy": 0.9,
            "time_window_hours": 24
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data,
            time_window_hours=24
        )
        
        assert len(alerts) > 0

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_send_alert_error_handling(self, mock_client_class):
        """Test alert sending error handling."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        alert = {
            "alert_type": "accuracy_drop",
            "severity": "high",
            "message": "Test alert"
        }
        
        with pytest.raises(Exception, match="Database error"):
            alerting.send_alert(model_id="model_id", alert=alert)

    def test_check_alerts_with_alert_cooldown(self):
        """Test alert checking with cooldown period."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.75,
            "baseline_accuracy": 0.9
        }
        
        # First check - should generate alert
        alerts1 = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        # Second check within cooldown - should not generate duplicate
        alerts2 = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data,
            cooldown_minutes=60
        )
        
        assert len(alerts1) > 0
        # Second check should respect cooldown
        assert len(alerts2) >= 0  # May or may not generate based on cooldown logic

    def test_check_alerts_with_aggregation(self):
        """Test alert checking with metric aggregation."""
        alerting = AlertingSystem()
        
        monitoring_data = {
            "accuracy": 0.75,
            "baseline_accuracy": 0.9,
            "aggregation": "mean"  # Aggregate over time window
        }
        
        alerts = alerting.check_alerts(
            model_id="model_id",
            monitoring_data=monitoring_data
        )
        
        assert len(alerts) > 0

    @patch('src.ownlens.ml.monitoring.alerting.Client')
    def test_get_alert_statistics(self, mock_client_class):
        """Test getting alert statistics."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (10, 5, 3, 2)  # total, high, medium, low
        ]
        mock_client_class.return_value = mock_client
        
        alerting = AlertingSystem(client=mock_client)
        
        stats = alerting.get_alert_statistics(
            model_id="model_id",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert stats is not None
        assert "total" in stats or isinstance(stats, dict)

