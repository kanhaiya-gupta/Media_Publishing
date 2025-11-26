"""
OwnLens - ML Module: Churn Predictor

Predictor for churn prediction model.
"""

from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.user_features_loader import UserFeaturesLoader
from .model import ChurnModel


class ChurnPredictor(BasePredictor):
    """
    Predictor for churn prediction model.
    
    Handles the prediction workflow:
    1. Load user features
    2. Prepare features
    3. Make predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[ChurnModel] = None):
        """
        Initialize churn predictor.
        
        Args:
            model: Churn model instance (if None, creates default)
        """
        if model is None:
            model = ChurnModel()
        
        super().__init__(model, None)
        self.logger.info("Initialized churn predictor")
    
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
        
        # Select feature columns (same as training)
        exclude_cols = [
            'user_feature_id', 'user_id', 'company_id', 'brand_id',
            'feature_date', 'created_at', 'updated_at', 'ml_features'
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
        Predict churn for users.
        
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
                columns=['prob_not_churned', 'prob_churned']
            )
            result['churn_probability'] = result['prob_churned']
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
        Predict churn for multiple users.
        
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
        result['churn_prediction'] = (predictions['churn_probability'] > 0.5).astype(int)
        result['churn_probability'] = predictions['churn_probability']
        result['churn_risk_level'] = pd.cut(
            predictions['churn_probability'],
            bins=[0, 0.3, 0.6, 0.8, 1.0],
            labels=['low', 'medium', 'high', 'critical']
        )
        
        return result

