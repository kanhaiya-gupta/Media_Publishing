"""
OwnLens - ML Module: Article Performance Predictor

Predictor for article performance prediction model.
"""

from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.content_features import ContentFeatureEngineer
from ....features.editorial.performance_features import PerformanceFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import ArticlePerformanceModel


class ArticlePerformancePredictor(BasePredictor):
    """
    Predictor for article performance prediction model.
    
    Handles the prediction workflow:
    1. Load article metadata
    2. Prepare features
    3. Make predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[ArticlePerformanceModel] = None):
        """
        Initialize article performance predictor.
        
        Args:
            model: Article performance model instance (if None, creates default)
        """
        if model is None:
            model = ArticlePerformanceModel()
        
        # Initialize feature engineers
        content_fe = ContentFeatureEngineer()
        performance_fe = PerformanceFeatureEngineer()
        feature_engineer = CompositeFeatureEngineer([content_fe, performance_fe])
        
        super().__init__(model, feature_engineer)
        self.logger.info("Initialized article performance predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction.
        
        Args:
            data: Article metadata DataFrame or article_id
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        # If data is an article_id, load metadata
        if isinstance(data, str):
            loader = EditorialLoader()
            articles = loader.load_articles_metadata(article_id=data, limit=1)
            if len(articles) == 0:
                raise ValueError(f"No article found with ID {data}")
            data = articles
        
        # Apply feature engineering
        if self.feature_engineer:
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (same as training)
        exclude_cols = [
            'article_id', 'external_article_id', 'company_id', 'brand_id',
            'performance_id', 'performance_date',
            'title', 'headline', 'subtitle', 'summary', 'content_url',
            'publish_time', 'publish_date', 'status',
            'created_at', 'updated_at',
            'primary_author_id', 'primary_category_id',
            'engagement_score', 'total_views', 'total_clicks'  # Target columns
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
        Predict article performance.
        
        Args:
            data: Article metadata DataFrame or article_id
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
        
        # Add article IDs if available
        if isinstance(data, pd.DataFrame) and 'article_id' in data.columns:
            result['article_id'] = data['article_id'].values
        
        return result
    
    def predict_batch(
        self,
        article_ids: list,
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict performance for multiple articles.
        
        Args:
            article_ids: List of article IDs
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with predictions
        """
        loader = EditorialLoader()
        articles = loader.load_articles_metadata(limit=len(article_ids))
        
        # Filter to requested articles
        if 'article_id' in articles.columns:
            articles = articles[articles['article_id'].isin(article_ids)]
        
        if len(articles) == 0:
            raise ValueError("No articles found for requested IDs")
        
        # Get predictions
        predictions = self.predict(articles, **kwargs)
        
        return predictions

