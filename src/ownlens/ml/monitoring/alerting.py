"""
OwnLens - ML Module: Alerting System

Alerting system for model monitoring.
"""

from typing import Any, Dict, Optional, List
from datetime import date
import json
import logging
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class AlertingSystem:
    """
    Alerting system for model monitoring.
    
    Handles:
    - Checking alert conditions
    - Sending alerts
    - Tracking alerts in ml_model_monitoring
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize alerting system.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized alerting system")
        
        # Alert thresholds
        self.thresholds = {
            'accuracy_drop': 0.1,  # 10% accuracy drop
            'drift_score': 0.2,  # 20% drift
            'error_rate': 0.05,  # 5% error rate
            'latency_ms': 1000.0  # 1 second latency
        }
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def check_alerts(
        self,
        model_id: str,
        monitoring_data: Dict[str, Any],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Check if any alerts should be triggered.
        
        Args:
            model_id: Model ID
            monitoring_data: Monitoring data dictionary
            **kwargs: Additional parameters
        
        Returns:
            List of alerts
        """
        alerts = []
        
        # Check accuracy threshold
        if 'accuracy' in monitoring_data and monitoring_data['accuracy'] is not None:
            baseline_accuracy = kwargs.get('baseline_accuracy', 0.9)
            current_accuracy = monitoring_data['accuracy']
            accuracy_drop = baseline_accuracy - current_accuracy
            
            if accuracy_drop > self.thresholds['accuracy_drop']:
                alerts.append({
                    'alert_type': 'accuracy_drop',
                    'severity': 'high',
                    'message': f"Model accuracy dropped by {accuracy_drop:.2%} (current: {current_accuracy:.4f}, baseline: {baseline_accuracy:.4f})",
                    'threshold': self.thresholds['accuracy_drop']
                })
        
        # Check drift threshold
        if 'data_drift_score' in monitoring_data and monitoring_data['data_drift_score'] is not None:
            drift_score = monitoring_data['data_drift_score']
            
            if drift_score > self.thresholds['drift_score']:
                alerts.append({
                    'alert_type': 'data_drift',
                    'severity': 'medium',
                    'message': f"Data drift detected: score={drift_score:.4f} (threshold: {self.thresholds['drift_score']})",
                    'threshold': self.thresholds['drift_score']
                })
        
        # Check error rate threshold
        if 'error_rate' in monitoring_data and monitoring_data['error_rate'] is not None:
            error_rate = monitoring_data['error_rate']
            
            if error_rate > self.thresholds['error_rate']:
                alerts.append({
                    'alert_type': 'high_error_rate',
                    'severity': 'high',
                    'message': f"High error rate: {error_rate:.2%} (threshold: {self.thresholds['error_rate']:.2%})",
                    'threshold': self.thresholds['error_rate']
                })
        
        # Check latency threshold
        if 'avg_prediction_latency_ms' in monitoring_data and monitoring_data['avg_prediction_latency_ms'] is not None:
            latency_ms = monitoring_data['avg_prediction_latency_ms']
            
            if latency_ms > self.thresholds['latency_ms']:
                alerts.append({
                    'alert_type': 'high_latency',
                    'severity': 'medium',
                    'message': f"High prediction latency: {latency_ms:.0f}ms (threshold: {self.thresholds['latency_ms']:.0f}ms)",
                    'threshold': self.thresholds['latency_ms']
                })
        
        return alerts
    
    def send_alert(
        self,
        alert_type: str,
        model_id: str,
        message: str,
        severity: str = 'medium',
        **kwargs
    ):
        """
        Send alert (email, Slack, etc.).
        
        Args:
            alert_type: Alert type
            model_id: Model ID
            message: Alert message
            severity: Alert severity ('low', 'medium', 'high', 'critical')
            **kwargs: Additional parameters
        """
        # Log alert
        self.logger.warning(f"ALERT [{severity.upper()}]: {alert_type} - {message} (Model: {model_id})")
        
        # TODO: Integrate with alerting channels (email, Slack, PagerDuty, etc.)
        # For now, we just log the alert
        
        # In production, you would:
        # - Send email notification
        # - Send Slack message
        # - Create PagerDuty incident
        # - etc.
    
    def daily_alert_check(
        self,
        model_id: str,
        monitoring_date: date,
        baseline_accuracy: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run daily alert checks and update ml_model_monitoring.
        
        Args:
            model_id: Model ID
            monitoring_date: Date for alert check
            baseline_accuracy: Baseline accuracy for comparison
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with alert results
        """
        client = self._get_client()
        
        try:
            # Get monitoring data
            # Format values directly - ClickHouse doesn't support parameterized SELECT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                if isinstance(value, date):
                    return f"'{value}'"
                return str(value)
            
            model_id_str = escape_sql(model_id)
            monitoring_date_str = escape_sql(monitoring_date) if monitoring_date else 'NULL'
            
            query = f"""
            SELECT 
                accuracy, data_drift_score, performance_drift_score,
                error_rate, avg_prediction_latency_ms
            FROM ml_model_monitoring
            WHERE model_id = {model_id_str} AND monitoring_date = {monitoring_date_str}
            LIMIT 1
            """
            
            results = client.execute(query)
            
            if not results:
                self.logger.warning(f"No monitoring data found for model {model_id} on {monitoring_date}")
                return {'alerts_triggered': 0, 'alerts': []}
            
            row = results[0]
            monitoring_data = {
                'accuracy': float(row[0]) if row[0] else None,
                'data_drift_score': float(row[1]) if row[1] else None,
                'performance_drift_score': float(row[2]) if row[2] else None,
                'error_rate': float(row[3]) if row[3] else None,
                'avg_prediction_latency_ms': float(row[4]) if row[4] else None
            }
            
            # Check alerts
            alerts = self.check_alerts(model_id, monitoring_data, baseline_accuracy=baseline_accuracy, **kwargs)
            
            # Send alerts
            alert_types = []
            for alert in alerts:
                self.send_alert(
                    alert_type=alert['alert_type'],
                    model_id=model_id,
                    message=alert['message'],
                    severity=alert['severity']
                )
                alert_types.append(alert['alert_type'])
            
            # Update monitoring table
            if alerts:
                # Format values directly - ClickHouse doesn't support parameterized ALTER TABLE UPDATE
                alerts_count = len(alerts)
                alert_types_json = escape_sql(json.dumps(alert_types))
                
                update_query = f"""
                ALTER TABLE ml_model_monitoring
                UPDATE 
                    alerts_triggered = {alerts_count},
                    alert_types = {alert_types_json},
                    updated_at = now()
                WHERE model_id = {model_id_str} AND monitoring_date = {monitoring_date_str}
                """
                
                client.execute(update_query)
            
            result = {
                'alerts_triggered': len(alerts),
                'alerts': alerts,
                'alert_types': alert_types
            }
            
            self.logger.info(f"Alert check completed for model {model_id}: {len(alerts)} alerts triggered")
            return result
            
        finally:
            if not self.client:
                client.disconnect()

