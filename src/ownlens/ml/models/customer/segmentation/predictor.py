"""
OwnLens - ML Module: User Segmentation Predictor

Predictor for user segmentation model.
"""

from typing import Any, Dict, Optional, Union, List
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.user_features_loader import UserFeaturesLoader
from ....features.customer.behavioral_features import BehavioralFeatureEngineer
from ....features.customer.engagement_features import EngagementFeatureEngineer
from ....features.customer.temporal_features import TemporalFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import UserSegmentationModel


class UserSegmentationPredictor(BasePredictor):
    """
    Predictor for user segmentation model.
    
    Handles the prediction workflow:
    1. Load user features
    2. Prepare features
    3. Predict segments
    4. Format results
    """
    
    def __init__(self, model: Optional[UserSegmentationModel] = None):
        """
        Initialize user segmentation predictor.
        
        Args:
            model: User segmentation model instance (if None, creates default)
        """
        if model is None:
            model = UserSegmentationModel()
        
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
        self.logger.info("Initialized user segmentation predictor")
    
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
            'first_session_date', 'last_session_date'  # Date columns
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        X = data[feature_cols].copy()
        
        # Handle categorical columns separately - convert to object type first
        categorical_cols = X.select_dtypes(include=['category']).columns
        if len(categorical_cols) > 0:
            X[categorical_cols] = X[categorical_cols].astype('object')
        
        # Drop object/string columns - K-Means requires numeric features only
        # These columns can be encoded separately if needed, but for now we drop them
        object_cols = X.select_dtypes(include=['object']).columns
        if len(object_cols) > 0:
            self.logger.info(f"Dropping {len(object_cols)} object columns for K-Means: {list(object_cols)}")
            X = X.drop(columns=object_cols)
        
        # Handle missing values for numeric columns
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return X
    
    def predict(
        self,
        data: Union[pd.DataFrame, str, List[str]],
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict user segments.
        
        Args:
            data: User features DataFrame, user_id, or list of user_ids
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with segment predictions
        """
        # Handle list of user IDs
        if isinstance(data, list):
            loader = UserFeaturesLoader()
            data = loader.load(limit=len(data))
            if 'user_id' in data.columns:
                data = data[data['user_id'].isin(data)]
        
        X = self.prepare_features(data, **kwargs)
        
        predictions = self.model.predict(X, **kwargs)
        
        # Return DataFrame with predictions
        result = pd.DataFrame({
            'segment_number': predictions,
            'segment_name': [self.model.segment_names.get(int(p), f'segment_{int(p)+1}') for p in predictions]
        })
        
        # Add user IDs if available
        if isinstance(data, pd.DataFrame) and 'user_id' in data.columns:
            result['user_id'] = data['user_id'].values
        
        # Add confidence scores (distance to cluster center)
        if hasattr(self.model.model, 'cluster_centers_'):
            centers = self.model.model.cluster_centers_
            X_scaled = self.model.scaler.transform(X)
            
            distances = []
            for i, pred in enumerate(predictions):
                center = centers[int(pred)]
                distance = np.linalg.norm(X_scaled[i] - center)
                distances.append(float(distance))
            
            result['distance_to_center'] = distances
            # Normalize confidence (lower distance = higher confidence)
            max_distance = max(distances) if distances else 1.0
            result['confidence_score'] = 1.0 - (result['distance_to_center'] / max_distance)
        
        return result
    
    def predict_batch(
        self,
        user_ids: List[str],
        feature_date: Optional[Any] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict segments for multiple users.
        
        Args:
            user_ids: List of user IDs
            feature_date: Feature date (default: today)
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with segment predictions
        """
        loader = UserFeaturesLoader()
        data = loader.load(feature_date=feature_date, limit=len(user_ids))
        
        # Filter to requested users
        if 'user_id' in data.columns:
            data = data[data['user_id'].isin(user_ids)]
        
        if len(data) == 0:
            raise ValueError("No features found for requested users")
        
        # Get predictions
        predictions = self.predict(data, **kwargs)
        
        return predictions

