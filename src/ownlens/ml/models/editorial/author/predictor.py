"""
OwnLens - ML Module: Author Performance Predictor

Predictor for author performance prediction model.
"""

from typing import Any, Dict, Optional, Union
from datetime import date
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.author_features import AuthorFeatureEngineer
from .model import AuthorPerformanceModel


class AuthorPerformancePredictor(BasePredictor):
    """
    Predictor for author performance prediction model.
    
    Handles the prediction workflow:
    1. Load author performance data
    2. Prepare features
    3. Make predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[AuthorPerformanceModel] = None):
        """
        Initialize author performance predictor.
        
        Args:
            model: Author performance model instance (if None, creates default)
        """
        if model is None:
            model = AuthorPerformanceModel()
        
        # Initialize feature engineers
        author_fe = AuthorFeatureEngineer()
        super().__init__(model, author_fe)
        self.logger.info("Initialized author performance predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction.
        
        Args:
            data: Author performance DataFrame or author_id
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        # If data is an author_id, load performance data
        if isinstance(data, str):
            loader = EditorialLoader()
            authors = loader.load_author_performance(author_id=data, limit=1)
            if len(authors) == 0:
                raise ValueError(f"No author found with ID {data}")
            data = authors
        
        # Apply feature engineering
        if self.feature_engineer:
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (same as training)
        exclude_cols = [
            'performance_id', 'author_id', 'brand_id', 'performance_date',
            'best_article_id', 'top_category_id',
            'created_at', 'updated_at',
            'avg_engagement_score', 'author_rank', 'engagement_rank'  # Target columns
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
        Predict author performance.
        
        Args:
            data: Author performance DataFrame or author_id
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        X = self.prepare_features(data, **kwargs)
        
        predictions = self.model.predict(X, **kwargs)
        
        # Return DataFrame with predictions
        result = pd.DataFrame({
            'predicted_engagement_score': predictions
        })
        
        # Add author IDs if available
        if isinstance(data, pd.DataFrame) and 'author_id' in data.columns:
            result['author_id'] = data['author_id'].values
        
        return result
    
    def predict_batch(
        self,
        author_ids: list,
        performance_date: Optional[date] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict performance for multiple authors.
        
        Args:
            author_ids: List of author IDs
            performance_date: Performance date (default: today)
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        loader = EditorialLoader()
        authors = loader.load_author_performance(
            performance_date=performance_date,
            limit=len(author_ids)
        )
        
        # Filter to requested authors
        if 'author_id' in authors.columns:
            authors = authors[authors['author_id'].isin(author_ids)]
        
        if len(authors) == 0:
            raise ValueError("No authors found for requested IDs")
        
        # Get predictions
        predictions = self.predict(authors, **kwargs)
        
        # Sort by predicted engagement
        predictions = predictions.sort_values('predicted_engagement_score', ascending=False)
        
        return predictions

