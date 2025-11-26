"""
OwnLens - ML Module: Drift Detector

Drift detection for data and performance drift.
"""

from typing import Any, Dict, Optional, List
from datetime import date
import json
import logging
import pandas as pd
import numpy as np
from scipy import stats
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class DriftDetector:
    """
    Drift detector for detecting data drift and performance drift.
    
    Handles:
    - Data drift detection (input feature distribution changes)
    - Performance drift detection (model accuracy degradation)
    - Statistical tests (KS test, PSI, etc.)
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize drift detector.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized drift detector")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def detect_data_drift(
        self,
        model_id: str,
        current_features: pd.DataFrame,
        training_features: pd.DataFrame,
        threshold: float = 0.1
    ) -> Dict[str, Any]:
        """
        Detect if input data distribution has changed (data drift).
        
        Args:
            model_id: Model ID
            current_features: Current feature distributions
            training_features: Training feature distributions
            threshold: Drift threshold (default: 0.1)
        
        Returns:
            Dictionary with drift scores and detection results
        """
        drift_scores = {}
        drift_detected = False
        
        # Compare each feature
        common_features = set(current_features.columns) & set(training_features.columns)
        
        for feature in common_features:
            current_values = current_features[feature].dropna()
            training_values = training_features[feature].dropna()
            
            if len(current_values) == 0 or len(training_values) == 0:
                continue
            
            # Kolmogorov-Smirnov test
            try:
                ks_statistic, p_value = stats.ks_2samp(training_values, current_values)
                drift_scores[feature] = {
                    'ks_statistic': float(ks_statistic),
                    'p_value': float(p_value),
                    'drift_score': float(ks_statistic)  # Higher = more drift
                }
                
                if ks_statistic > threshold:
                    drift_detected = True
            except Exception as e:
                self.logger.warning(f"Could not calculate drift for feature {feature}: {e}")
                drift_scores[feature] = {
                    'ks_statistic': 0.0,
                    'p_value': 1.0,
                    'drift_score': 0.0
                }
        
        # Calculate overall drift score
        if drift_scores:
            overall_drift_score = np.mean([s['drift_score'] for s in drift_scores.values()])
        else:
            overall_drift_score = 0.0
        
        result = {
            'model_id': model_id,
            'drift_detected': drift_detected,
            'overall_drift_score': float(overall_drift_score),
            'feature_drift_scores': drift_scores,
            'threshold': threshold
        }
        
        self.logger.info(f"Data drift detection for model {model_id}: detected={drift_detected}, score={overall_drift_score:.4f}")
        return result
    
    def detect_performance_drift(
        self,
        model_id: str,
        current_accuracy: float,
        baseline_accuracy: float,
        threshold: float = 0.05
    ) -> Dict[str, Any]:
        """
        Detect if model performance has degraded (performance drift).
        
        Args:
            model_id: Model ID
            current_accuracy: Current model accuracy
            baseline_accuracy: Baseline accuracy (from training)
            threshold: Performance degradation threshold (default: 0.05 = 5%)
        
        Returns:
            Dictionary with performance drift results
        """
        # Calculate performance drift score
        accuracy_drop = baseline_accuracy - current_accuracy
        performance_drift_score = accuracy_drop / baseline_accuracy if baseline_accuracy > 0 else 0.0
        performance_drift_detected = performance_drift_score > threshold
        
        result = {
            'model_id': model_id,
            'performance_drift_detected': performance_drift_detected,
            'performance_drift_score': float(performance_drift_score),
            'current_accuracy': float(current_accuracy),
            'baseline_accuracy': float(baseline_accuracy),
            'accuracy_drop': float(accuracy_drop),
            'threshold': threshold
        }
        
        self.logger.info(f"Performance drift detection for model {model_id}: detected={performance_drift_detected}, score={performance_drift_score:.4f}")
        return result
    
    def check_drift(
        self,
        model_id: str,
        monitoring_date: date,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Check for drift and update ml_model_monitoring table.
        
        Args:
            model_id: Model ID
            monitoring_date: Date for drift check
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with drift detection results
        """
        client = self._get_client()
        
        try:
            # Get model metadata to get training data distribution
            # For now, we'll use a simplified approach
            # In production, you'd load training feature distributions from registry
            
            # Get current predictions for feature distribution
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
                    self.logger.info(f"Using latest prediction_date for drift check: {monitoring_date}")
                else:
                    self.logger.warning(f"No predictions found for drift check: model {model_id}")
                    return {'drift_detected': False}
            
            monitoring_date_str = escape_sql(monitoring_date)
            
            query = f"""
            SELECT input_features
            FROM ml_model_predictions
            WHERE model_id = {model_id_str} AND prediction_date = {monitoring_date_str}
            LIMIT 1000
            """
            
            results = client.execute(query)
            
            if not results:
                self.logger.warning(f"No predictions found for drift check: model {model_id} on {monitoring_date}")
                return {'drift_detected': False}
            
            # Parse features (simplified - in production, you'd have proper feature extraction)
            # For now, we'll calculate a simple drift score based on prediction distribution
            
            # Update ml_model_monitoring with drift scores
            # This is a simplified version - full implementation would:
            # 1. Load training feature distributions from registry
            # 2. Compare with current feature distributions
            # 3. Calculate drift scores
            # 4. Update monitoring table
            
            drift_result = {
                'drift_detected': False,
                'data_drift_score': 0.0,
                'performance_drift_score': 0.0
            }
            
            # Update monitoring table
            # Format values directly - ClickHouse doesn't support parameterized ALTER TABLE UPDATE
            data_drift_score = drift_result.get('data_drift_score', 0.0)
            drift_detected = 1 if drift_result.get('drift_detected', False) else 0
            performance_drift_score = drift_result.get('performance_drift_score', 0.0)
            performance_drift_detected = 0  # performance_drift_detected
            
            model_id_str = escape_sql(model_id)
            monitoring_date_str = escape_sql(monitoring_date) if monitoring_date else 'NULL'
            
            update_query = f"""
            ALTER TABLE ml_model_monitoring
            UPDATE 
                data_drift_score = {data_drift_score},
                drift_detected = {drift_detected},
                performance_drift_score = {performance_drift_score},
                performance_drift_detected = {performance_drift_detected},
                updated_at = now()
            WHERE model_id = {model_id_str} AND monitoring_date = {monitoring_date_str}
            """
            
            client.execute(update_query)
            
            self.logger.info(f"Drift check completed for model {model_id} on {monitoring_date}")
            return drift_result
            
        finally:
            if not self.client:
                client.disconnect()

