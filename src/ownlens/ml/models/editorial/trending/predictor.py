"""
OwnLens - ML Module: Trending Topics Predictor

Predictor for trending topics detection model.
"""

from typing import Any, Dict, Optional, Union
from datetime import date
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.performance_features import PerformanceFeatureEngineer
from .model import TrendingTopicsModel


class TrendingTopicsPredictor(BasePredictor):
    """
    Predictor for trending topics detection model.
    
    Handles the prediction workflow:
    1. Load category performance data
    2. Prepare features
    3. Make predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[TrendingTopicsModel] = None):
        """
        Initialize trending topics predictor.
        
        Args:
            model: Trending topics model instance (if None, creates default)
        """
        if model is None:
            model = TrendingTopicsModel()
        
        # Initialize feature engineers
        performance_fe = PerformanceFeatureEngineer()
        super().__init__(model, performance_fe)
        self.logger.info("Initialized trending topics predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction.
        
        Args:
            data: Category performance DataFrame or category_id
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        # If data is a category_id, load performance data
        if isinstance(data, str):
            loader = EditorialLoader()
            categories = loader.load_category_performance(category_id=data, limit=1)
            if len(categories) == 0:
                raise ValueError(f"No category found with ID {data}")
            data = categories
        
        # Apply feature engineering
        if self.feature_engineer:
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (same as training)
        exclude_cols = [
            'performance_id', 'category_id', 'brand_id', 'performance_date',
            'best_article_id', 'top_author_ids',
            'created_at', 'updated_at',
            'trending_score', 'is_trending', 'trending_rank'  # Target columns
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
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict trending score for categories.
        
        Args:
            data: Category performance DataFrame or category_id
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        X = self.prepare_features(data, **kwargs)
        
        predictions = self.model.predict(X, **kwargs)
        
        # Return DataFrame with predictions
        result = pd.DataFrame({
            'predicted_trending_score': predictions
        })
        
        # Add category IDs if available
        if isinstance(data, pd.DataFrame) and 'category_id' in data.columns:
            result['category_id'] = data['category_id'].values
        
        # Add trending classification
        result['is_trending'] = (result['predicted_trending_score'] > result['predicted_trending_score'].quantile(0.75)).astype(int)
        
        return result
    
    def predict_batch(
        self,
        category_ids: list,
        performance_date: Optional[date] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict trending scores for multiple categories.
        
        Args:
            category_ids: List of category IDs
            performance_date: Performance date (default: today)
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        loader = EditorialLoader()
        categories = loader.load_category_performance(
            performance_date=performance_date,
            limit=len(category_ids)
        )
        
        # Filter to requested categories
        if 'category_id' in categories.columns:
            categories = categories[categories['category_id'].isin(category_ids)]
        
        if len(categories) == 0:
            raise ValueError("No categories found for requested IDs")
        
        # Get predictions
        predictions = self.predict(categories, **kwargs)
        
        # Sort by trending score
        predictions = predictions.sort_values('predicted_trending_score', ascending=False)
        
        return predictions

