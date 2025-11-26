"""
OwnLens - ML Module: Conversion Predictor

Predictor for conversion prediction model.
"""

from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.user_features_loader import UserFeaturesLoader
from ....features.customer.behavioral_features import BehavioralFeatureEngineer
from ....features.customer.engagement_features import EngagementFeatureEngineer
from ....features.customer.temporal_features import TemporalFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import ConversionModel


class ConversionPredictor(BasePredictor):
    """
    Predictor for conversion prediction model.
    
    Handles the prediction workflow:
    1. Load user features
    2. Prepare features
    3. Make predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[ConversionModel] = None):
        """
        Initialize conversion predictor.
        
        Args:
            model: Conversion model instance (if None, creates default)
        """
        if model is None:
            model = ConversionModel()
        
        # Initialize feature engineers
        behavioral_fe = BehavioralFeatureEngineer()
        engagement_fe = EngagementFeatureEngineer()
        temporal_fe = TemporalFeatureEngineer()
        feature_engineer = CompositeFeatureEngineer([
            behavioral_fe,
            engagement_fe,
            temporal_fe
        ])
        
        super().__init__(model, feature_engineer)
        self.logger.info("Initialized conversion predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction.
        
        Args:
            data: User features DataFrame or user_id
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        # If data is a user_id, load features
        if isinstance(data, str):
            loader = UserFeaturesLoader()
            data = loader.load(limit=1)  # Load single user
            if len(data) == 0:
                raise ValueError(f"No features found for user {data}")
        
        # Apply feature engineering
        if self.feature_engineer:
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (same as training)
        exclude_cols = [
            'user_feature_id', 'user_id', 'company_id', 'brand_id',
            'feature_date', 'created_at', 'updated_at', 'ml_features',
            'first_session_date', 'last_session_date',  # Date columns
            'current_subscription_tier', 'subscription_tier'  # Target columns
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        X = data[feature_cols].copy()
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return X
    
    def predict(
        self,
        data: Union[pd.DataFrame, str],
        return_proba: bool = False,
        **kwargs
    ) -> Union[np.ndarray, pd.DataFrame]:
        """
        Predict conversion for users.
        
        Args:
            data: User features DataFrame or user_id
            return_proba: If True, return probabilities instead of binary predictions
            **kwargs: Additional parameters
        
        Returns:
            Predictions or probabilities
        """
        X = self.prepare_features(data, **kwargs)
        
        if return_proba:
            probabilities = self.model.predict_proba(X, **kwargs)
            # Return DataFrame with probabilities
            result = pd.DataFrame(
                probabilities,
                columns=['prob_not_converted', 'prob_converted']
            )
            result['conversion_probability'] = result['prob_converted']
            result['conversion_tier'] = pd.cut(
                result['conversion_probability'],
                bins=[0, 0.3, 0.6, 0.8, 1.0],
                labels=['low', 'medium', 'high', 'very_high']
            )
            return result
        else:
            predictions = self.model.predict(X, **kwargs)
            return predictions
    
    def predict_batch(
        self,
        user_ids: list,
        feature_date: Optional[Any] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict conversion for multiple users.
        
        Args:
            user_ids: List of user IDs
            feature_date: Feature date (default: today)
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        loader = UserFeaturesLoader()
        data = loader.load(feature_date=feature_date, limit=len(user_ids))
        
        # Filter to requested users
        if 'user_id' in data.columns:
            data = data[data['user_id'].isin(user_ids)]
        
        if len(data) == 0:
            raise ValueError("No features found for requested users")
        
        # Get predictions
        predictions = self.predict(data, return_proba=True, **kwargs)
        
        # Combine with user IDs
        result = data[['user_id']].copy()
        result['conversion_prediction'] = (predictions['conversion_probability'] > 0.5).astype(int)
        result['conversion_probability'] = predictions['conversion_probability']
        result['conversion_tier'] = predictions['conversion_tier']
        
        return result

