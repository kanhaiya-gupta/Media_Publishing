"""
OwnLens - ML Module: Prediction Validator

Prediction validation utilities for comparing predictions vs actuals.
"""

from typing import Optional, Dict, Any
from datetime import date, datetime, timedelta
import logging
import pandas as pd
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config
from ownlens.ml.storage.prediction_storage import PredictionStorage


logger = logging.getLogger(__name__)


class PredictionValidator:
    """
    Prediction validator for validating predictions against actual outcomes.
    
    Handles:
    - Validating churn predictions
    - Validating conversion predictions
    - Calculating accuracy metrics
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize prediction validator.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.storage = PredictionStorage(client)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized prediction validator")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def validate_churn_prediction(
        self,
        prediction_id: str,
        user_id: str,
        validation_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Validate churn prediction by checking if user actually churned.
        
        Args:
            prediction_id: Prediction ID
            user_id: User ID
            validation_date: Date to check churn (default: today)
        
        Returns:
            Dictionary with validation results
        """
        if validation_date is None:
            validation_date = date.today()
        
        client = self._get_client()
        
        try:
            # Get user's current days_since_last_session
            from ownlens.ml.data.loaders.user_features_loader import UserFeaturesLoader
            loader = UserFeaturesLoader(client)
            user_features = loader.load(limit=1)
            
            if len(user_features) == 0 or user_id not in user_features['user_id'].values:
                self.logger.warning(f"User {user_id} not found for validation")
                return {'validated': False, 'error': 'User not found'}
            
            user_data = user_features[user_features['user_id'] == user_id].iloc[0]
            days_since_last_session = user_data.get('days_since_last_session', 0)
            
            # Check if user churned (30+ days inactive)
            is_churned = int(days_since_last_session >= 30)
            churned_date = validation_date if is_churned else None
            
            # Update prediction
            query = """
            ALTER TABLE customer_churn_predictions
            UPDATE 
                is_churned = ?,
                churned_date = ?,
                updated_at = now()
            WHERE prediction_id = ?
            """
            
            client.execute(query, [is_churned, churned_date, prediction_id])
            
            # Also update unified table
            self.storage.update_prediction_actual(
                prediction_id=prediction_id,
                actual_class='churned' if is_churned else 'not_churned',
                actual_value=float(is_churned)
            )
            
            validation_result = {
                'validated': True,
                'prediction_id': prediction_id,
                'user_id': user_id,
                'is_churned': bool(is_churned),
                'churned_date': churned_date,
                'days_since_last_session': int(days_since_last_session)
            }
            
            self.logger.info(f"Validated churn prediction {prediction_id}: churned={is_churned}")
            return validation_result
            
        finally:
            if not self.client:
                client.disconnect()
    
    def validate_conversion_prediction(
        self,
        prediction_id: str,
        user_id: str,
        validation_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Validate conversion prediction by checking if user actually converted.
        
        Args:
            prediction_id: Prediction ID
            user_id: User ID
            validation_date: Date to check conversion (default: today)
        
        Returns:
            Dictionary with validation results
        """
        if validation_date is None:
            validation_date = date.today()
        
        client = self._get_client()
        
        try:
            # Get user's current subscription tier
            from ownlens.ml.data.loaders.user_features_loader import UserFeaturesLoader
            loader = UserFeaturesLoader(client)
            user_features = loader.load(limit=1)
            
            if len(user_features) == 0 or user_id not in user_features['user_id'].values:
                self.logger.warning(f"User {user_id} not found for validation")
                return {'validated': False, 'error': 'User not found'}
            
            user_data = user_features[user_features['user_id'] == user_id].iloc[0]
            subscription_tier = user_data.get('current_subscription_tier', user_data.get('subscription_tier', 'free'))
            
            # Check if user converted (premium/pro/enterprise)
            converted_tiers = ['premium', 'pro', 'enterprise']
            did_convert = int(subscription_tier in converted_tiers)
            converted_at = validation_date if did_convert else None
            actual_tier = subscription_tier if did_convert else None
            
            # Update prediction
            query = """
            ALTER TABLE customer_conversion_predictions
            UPDATE 
                did_convert = ?,
                converted_at = ?,
                actual_tier = ?,
                updated_at = now()
            WHERE prediction_id = ?
            """
            
            client.execute(query, [did_convert, converted_at, actual_tier, prediction_id])
            
            # Also update unified table
            self.storage.update_prediction_actual(
                prediction_id=prediction_id,
                actual_class=subscription_tier,
                actual_value=float(did_convert)
            )
            
            validation_result = {
                'validated': True,
                'prediction_id': prediction_id,
                'user_id': user_id,
                'did_convert': bool(did_convert),
                'converted_at': converted_at,
                'actual_tier': actual_tier
            }
            
            self.logger.info(f"Validated conversion prediction {prediction_id}: converted={did_convert}")
            return validation_result
            
        finally:
            if not self.client:
                client.disconnect()
    
    def calculate_accuracy(
        self,
        model_id: str,
        prediction_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Calculate model accuracy from predictions vs actuals.
        
        Args:
            model_id: Model ID
            prediction_type: Prediction type ('churn', 'conversion', etc.)
            start_date: Start date filter
            end_date: End date filter
        
        Returns:
            Dictionary with accuracy metrics
        """
        client = self._get_client()
        
        try:
            query = """
            SELECT 
                COUNT(*) as total_predictions,
                SUM(is_correct) as correct_predictions,
                AVG(CASE WHEN is_correct IS NOT NULL THEN is_correct ELSE NULL END) as accuracy
            FROM ml_model_predictions
            WHERE model_id = ? AND prediction_type = ?
            """
            
            params = [model_id, prediction_type]
            
            if start_date:
                query += " AND prediction_date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND prediction_date <= ?"
                params.append(end_date)
            
            results = client.execute(query, params)
            
            if not results or results[0][0] == 0:
                return {
                    'total_predictions': 0,
                    'correct_predictions': 0,
                    'accuracy': 0.0
                }
            
            total = results[0][0]
            correct = results[0][1] or 0
            accuracy = float(results[0][2]) if results[0][2] else 0.0
            
            metrics = {
                'total_predictions': int(total),
                'correct_predictions': int(correct),
                'incorrect_predictions': int(total - correct),
                'accuracy': accuracy
            }
            
            self.logger.info(f"Calculated accuracy for model {model_id}: {accuracy:.4f}")
            return metrics
            
        finally:
            if not self.client:
                client.disconnect()

