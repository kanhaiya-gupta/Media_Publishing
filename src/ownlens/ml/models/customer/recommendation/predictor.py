"""
OwnLens - ML Module: User Recommendation Predictor

Predictor for user content recommendation model.
"""

from typing import Any, Dict, Optional, List, Union
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from .model import UserRecommendationModel


class UserRecommendationPredictor(BasePredictor):
    """
    Predictor for user content recommendation model.
    
    Handles the recommendation workflow:
    1. Get user ID
    2. Find recommended articles
    3. Format results
    """
    
    def __init__(self, model: Optional[UserRecommendationModel] = None):
        """
        Initialize user recommendation predictor.
        
        Args:
            model: User recommendation model instance (if None, creates default)
        """
        if model is None:
            model = UserRecommendationModel()
        
        super().__init__(model, None)
        self.logger.info("Initialized user recommendation predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str, List[str]],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction (required by BasePredictor).
        
        For recommendation models, this just returns the data as-is
        since the model handles user IDs directly.
        
        Args:
            data: User IDs (list, string, or DataFrame)
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with user IDs
        """
        if isinstance(data, str):
            # Single user ID
            return pd.DataFrame({'user_id': [data]})
        elif isinstance(data, list):
            # List of user IDs
            return pd.DataFrame({'user_id': data})
        elif isinstance(data, pd.DataFrame):
            # DataFrame - return as-is
            return data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def recommend(
        self,
        user_id: str,
        top_n: int = 10,
        exclude_interacted: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """
        Recommend articles for a user.
        
        Args:
            user_id: User ID
            top_n: Number of recommendations
            exclude_interacted: If True, exclude articles user has already interacted with
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with recommended articles
        """
        recommendations = self.model.recommend(
            user_id=user_id,
            top_n=top_n,
            exclude_interacted=exclude_interacted,
            **kwargs
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(recommendations)
        df['user_id'] = user_id
        df['rank'] = range(1, len(df) + 1)
        
        # Sort by recommendation score
        df = df.sort_values('recommendation_score', ascending=False)
        
        return df
    
    def recommend_batch(
        self,
        user_ids: List[str],
        top_n: int = 10,
        **kwargs
    ) -> pd.DataFrame:
        """
        Recommend articles for multiple users.
        
        Args:
            user_ids: List of user IDs
            top_n: Number of recommendations per user
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with recommendations for all users
        """
        df = self.model.recommend_batch(
            user_ids=user_ids,
            top_n=top_n,
            **kwargs
        )
        
        if len(df) == 0:
            return pd.DataFrame()
        
        # Add rank per user
        df['rank'] = df.groupby('user_id')['recommendation_score'].rank(ascending=False, method='first').astype(int)
        
        # Sort by user_id and rank
        df = df.sort_values(['user_id', 'rank'])
        
        return df

