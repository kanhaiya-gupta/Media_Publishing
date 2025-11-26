"""
OwnLens - ML Module: Performance Monitor

Performance monitoring for tracking model performance over time.
"""

from typing import Any, Dict, Optional, List
from datetime import date, datetime
import uuid
import json
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """
    Performance monitor for tracking model performance over time.
    
    Handles:
    - Tracking individual predictions
    - Calculating daily performance metrics
    - Generating monitoring reports
    - Saving to ml_model_monitoring table
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize performance monitor.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized performance monitor")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def track_prediction(
        self,
        model_id: str,
        prediction: Any,
        latency_ms: float,
        timestamp: Optional[datetime] = None,
        **kwargs
    ):
        """
        Track a single prediction (for real-time monitoring).
        
        Note: This is a lightweight tracking method. Full metrics
        are calculated in generate_monitoring_report().
        
        Args:
            model_id: Model ID
            prediction: Prediction value or probability
            latency_ms: Prediction latency in milliseconds
            timestamp: Prediction timestamp (default: now)
            **kwargs: Additional parameters
        """
        # This can be used for real-time tracking
        # For now, we'll aggregate in daily reports
        pass
    
    def calculate_accuracy(
        self,
        model_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, float]:
        """
        Calculate model accuracy from predictions vs actuals.
        
        Args:
            model_id: Model ID
            start_date: Start date filter
            end_date: End date filter
        
        Returns:
            Dictionary with accuracy metrics
        """
        client = self._get_client()
        
        try:
            # Format values directly - ClickHouse doesn't support parameterized SELECT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    # Escape single quotes by doubling them
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                if isinstance(value, date):
                    return f"'{value}'"
                return str(value)
            
            model_id_str = escape_sql(model_id)
            
            query = f"""
            SELECT 
                prediction_type,
                COUNT(*) as total,
                SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) as correct,
                AVG(CASE WHEN is_correct IS NOT NULL THEN is_correct ELSE NULL END) as accuracy
            FROM ml_model_predictions
            WHERE model_id = {model_id_str} AND is_correct IS NOT NULL
            """
            
            if start_date:
                start_date_str = escape_sql(start_date)
                query += f" AND prediction_date >= {start_date_str}"
            
            if end_date:
                end_date_str = escape_sql(end_date)
                query += f" AND prediction_date <= {end_date_str}"
            
            query += " GROUP BY prediction_type"
            
            results = client.execute(query)
            
            metrics = {}
            for row in results:
                pred_type = row[0]
                total = row[1]
                correct = row[2] or 0
                accuracy = float(row[3]) if row[3] else 0.0
                
                metrics[pred_type] = {
                    'total': int(total),
                    'correct': int(correct),
                    'accuracy': accuracy
                }
            
            return metrics
            
        finally:
            if not self.client:
                client.disconnect()
    
    def generate_monitoring_report(
        self,
        model_id: str,
        monitoring_date: date,
        **kwargs
    ) -> str:
        """
        Generate daily monitoring report and save to ml_model_monitoring.
        
        Args:
            model_id: Model ID
            monitoring_date: Date for monitoring report
            **kwargs: Additional parameters
        
        Returns:
            monitoring_id
        """
        client = self._get_client()
        
        try:
            # Generate monitoring_id
            monitoring_id = str(uuid.uuid4())
            
            # Calculate prediction metrics
            # Format values directly - ClickHouse doesn't support parameterized SELECT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    # Escape single quotes by doubling them
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                if isinstance(value, date):
                    return f"'{value}'"
                return str(value)
            
            model_id_str = escape_sql(model_id)
            
            # If monitoring_date is None, use the latest prediction_date from the table
            if monitoring_date is None:
                # Get latest prediction_date for this model
                latest_date_query = f"""
                SELECT MAX(prediction_date) 
                FROM ml_model_predictions 
                WHERE model_id = {model_id_str}
                """
                latest_date_result = client.execute(latest_date_query)
                if latest_date_result and latest_date_result[0][0] and latest_date_result[0][0] != date(1970, 1, 1):
                    monitoring_date = latest_date_result[0][0]
                    self.logger.info(f"Using latest prediction_date: {monitoring_date}")
                else:
                    self.logger.warning(f"No predictions found for model {model_id}")
                    return monitoring_id
            
            monitoring_date_str = escape_sql(monitoring_date)
            
            query = f"""
            SELECT 
                COUNT(*) as total_predictions,
                COUNT(*) / 24.0 as predictions_per_hour,
                AVG(toUnixTimestamp(predicted_at) - toUnixTimestamp(created_at)) * 1000 as avg_latency_ms
            FROM ml_model_predictions
            WHERE model_id = {model_id_str} AND prediction_date = {monitoring_date_str}
            """
            
            results = client.execute(query)
            
            if not results or results[0][0] == 0:
                self.logger.warning(f"No predictions found for model {model_id} on {monitoring_date}")
                return monitoring_id
            
            total_predictions = results[0][0]
            predictions_per_hour = float(results[0][1]) if results[0][1] else 0.0
            avg_latency_ms = float(results[0][2]) if results[0][2] else 0.0
            
            # Calculate accuracy metrics (if actuals available)
            accuracy_metrics = self.calculate_accuracy(model_id, monitoring_date, monitoring_date)
            
            # Get accuracy for primary prediction type
            primary_type = list(accuracy_metrics.keys())[0] if accuracy_metrics else None
            accuracy = accuracy_metrics[primary_type]['accuracy'] if primary_type else None
            precision = None  # Can be calculated from confusion matrix
            recall = None
            f1_score = None
            auc_roc = None
            
            # Calculate prediction distribution
            dist_query = f"""
            SELECT 
                prediction_probability,
                COUNT(*) as count
            FROM ml_model_predictions
            WHERE model_id = {model_id_str} AND prediction_date = {monitoring_date_str} AND prediction_probability IS NOT NULL
            GROUP BY prediction_probability
            ORDER BY prediction_probability
            """
            
            dist_results = client.execute(dist_query)
            prediction_distribution = {str(row[0]): int(row[1]) for row in dist_results} if dist_results else {}
            
            # Calculate percentiles for latency (simplified)
            p95_latency_ms = avg_latency_ms * 1.5  # Simplified
            p99_latency_ms = avg_latency_ms * 2.0  # Simplified
            
            # Insert into ml_model_monitoring
            # Format values directly - ClickHouse doesn't support parameterized INSERT queries
            def format_value(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    # Escape single quotes by doubling them
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                if isinstance(value, (int, float)):
                    return str(value)
                if isinstance(value, bool):
                    return '1' if value else '0'
                if isinstance(value, date):
                    return f"'{value}'"
                # For other types, convert to string and escape
                escaped = str(value).replace("'", "''")
                return f"'{escaped}'"
            
            monitoring_id_str = format_value(monitoring_id)
            model_id_str = format_value(model_id)
            monitoring_date_str = format_value(monitoring_date)
            total_predictions_str = format_value(int(total_predictions))
            predictions_per_hour_str = format_value(float(predictions_per_hour))
            avg_latency_str = format_value(float(avg_latency_ms))
            p95_latency_str = format_value(float(p95_latency_ms))
            p99_latency_str = format_value(float(p99_latency_ms))
            accuracy_str = format_value(accuracy)
            precision_str = format_value(precision)
            recall_str = format_value(recall)
            f1_score_str = format_value(f1_score)
            auc_roc_str = format_value(auc_roc)
            data_drift_score_str = format_value(None)
            feature_drift_scores_str = format_value('')
            drift_detected_str = format_value(0)
            performance_drift_score_str = format_value(None)
            performance_drift_detected_str = format_value(0)
            prediction_distribution_str = format_value(json.dumps(prediction_distribution))
            confidence_distribution_str = format_value('')
            error_count_str = format_value(0)
            error_rate_str = format_value(0.0)
            error_types_str = format_value('')
            alerts_triggered_str = format_value(0)
            alert_types_str = format_value('')
            company_id_str = format_value(kwargs.get('company_id', ''))
            brand_id_str = format_value(kwargs.get('brand_id', ''))
            metadata_str = format_value(json.dumps(kwargs.get('metadata', {})))
            
            query = f"""
            INSERT INTO ml_model_monitoring (
                monitoring_id, model_id,
                monitoring_date, monitoring_start_time, monitoring_end_time,
                total_predictions, predictions_per_hour,
                avg_prediction_latency_ms, p95_prediction_latency_ms, p99_prediction_latency_ms,
                accuracy, precision, recall, f1_score, auc_roc,
                data_drift_score, feature_drift_scores, drift_detected,
                performance_drift_score, performance_drift_detected,
                prediction_distribution, confidence_distribution,
                error_count, error_rate, error_types,
                alerts_triggered, alert_types,
                company_id, brand_id,
                metadata,
                created_at, updated_at
            ) VALUES (
                {monitoring_id_str}, {model_id_str},
                {monitoring_date_str}, now(), now(),
                {total_predictions_str}, {predictions_per_hour_str},
                {avg_latency_str}, {p95_latency_str}, {p99_latency_str},
                {accuracy_str}, {precision_str}, {recall_str}, {f1_score_str}, {auc_roc_str},
                {data_drift_score_str}, {feature_drift_scores_str}, {drift_detected_str},
                {performance_drift_score_str}, {performance_drift_detected_str},
                {prediction_distribution_str}, {confidence_distribution_str},
                {error_count_str}, {error_rate_str}, {error_types_str},
                {alerts_triggered_str}, {alert_types_str},
                {company_id_str}, {brand_id_str},
                {metadata_str},
                now(), now()
            )
            """
            
            client.execute(query)
            
            self.logger.info(f"Generated monitoring report: {monitoring_id} for model {model_id} on {monitoring_date}")
            return monitoring_id
            
        finally:
            if not self.client:
                client.disconnect()

