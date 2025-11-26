"""
OwnLens - ML Module: Headline Optimization Predictor

Predictor for headline optimization model.
"""

from typing import Any, Dict, Optional, Union, List
import pandas as pd
import numpy as np

from ....base.predictor import BasePredictor
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.content_features import ContentFeatureEngineer
from .model import HeadlineOptimizationModel


class HeadlineOptimizationPredictor(BasePredictor):
    """
    Predictor for headline optimization model.
    
    Handles the prediction workflow:
    1. Load article metadata
    2. Prepare features
    3. Make CTR predictions
    4. Format results
    """
    
    def __init__(self, model: Optional[HeadlineOptimizationModel] = None):
        """
        Initialize headline optimization predictor.
        
        Args:
            model: Headline optimization model instance (if None, creates default)
        """
        if model is None:
            model = HeadlineOptimizationModel()
        
        # Initialize feature engineers
        content_fe = ContentFeatureEngineer()
        super().__init__(model, content_fe)
        self.logger.info("Initialized headline optimization predictor")
    
    def prepare_features(
        self,
        data: Union[pd.DataFrame, str, Dict[str, str]],
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for prediction.
        
        Args:
            data: Article metadata DataFrame, article_id, or dict with headline/article info
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        # If data is a dict with headline/article info
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        
        # If data is an article_id, load metadata
        elif isinstance(data, str):
            loader = EditorialLoader()
            articles = loader.load_articles_metadata(article_id=data, limit=1)
            if len(articles) == 0:
                raise ValueError(f"No article found with ID {data}")
            data = articles
        
        # Apply feature engineering
        if self.feature_engineer:
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Create headline features
        if 'headline' in data.columns:
            # Headline length
            data['headline_length'] = data['headline'].str.len().fillna(0)
            data['headline_word_count'] = data['headline'].str.split().str.len().fillna(0)
            
            # Headline features
            data['headline_has_question'] = data['headline'].str.contains('?', na=False).astype(int)
            data['headline_has_number'] = data['headline'].str.contains(r'\d', na=False, regex=True).astype(int)
            data['headline_has_exclamation'] = data['headline'].str.contains('!', na=False).astype(int)
        
        # Select feature columns (same as training)
        exclude_cols = [
            'article_id', 'external_article_id', 'company_id', 'brand_id',
            'performance_id', 'performance_date',
            'headline', 'title', 'subtitle', 'summary', 'content_url',
            'publish_time', 'publish_date', 'status',
            'created_at', 'updated_at',
            'total_clicks', 'total_views'  # Target columns
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        X = data[feature_cols].copy()
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return X
    
    def predict_ctr(
        self,
        data: Union[pd.DataFrame, str, Dict[str, str]],
        **kwargs
    ) -> pd.DataFrame:
        """
        Predict CTR for headlines.
        
        Args:
            data: Article metadata DataFrame, article_id, or dict with headline/article info
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with CTR predictions
        """
        X = self.prepare_features(data, **kwargs)
        
        predictions = self.model.predict_ctr(X, **kwargs)
        
        # Return DataFrame with predictions
        result = pd.DataFrame({
            'predicted_ctr': predictions
        })
        
        # Add headline if available
        if isinstance(data, pd.DataFrame) and 'headline' in data.columns:
            result['headline'] = data['headline'].values
        
        if isinstance(data, pd.DataFrame) and 'article_id' in data.columns:
            result['article_id'] = data['article_id'].values
        
        return result
    
    def compare_headlines(
        self,
        article_id: str,
        headlines: List[str],
        **kwargs
    ) -> pd.DataFrame:
        """
        Compare multiple headline variants for an article.
        
        Args:
            article_id: Article ID
            headlines: List of headline variants to compare
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with CTR predictions for each headline
        """
        loader = EditorialLoader()
        article = loader.load_articles_metadata(article_id=article_id, limit=1)
        
        if len(article) == 0:
            raise ValueError(f"No article found with ID {article_id}")
        
        # Create variants
        variants = []
        for headline in headlines:
            variant = article.copy()
            variant['headline'] = headline
            variants.append(variant)
        
        variants_df = pd.concat(variants, ignore_index=True)
        
        # Predict CTR for each variant
        predictions = self.predict_ctr(variants_df, **kwargs)
        
        # Sort by predicted CTR
        predictions = predictions.sort_values('predicted_ctr', ascending=False)
        
        return predictions

