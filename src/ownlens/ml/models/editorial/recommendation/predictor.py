"""
OwnLens - ML Module: Content Recommendation Predictor

Predictor for content recommendation model.
"""

from typing import Any, Dict, Optional, List, Union
import pandas as pd

from ....base.predictor import BasePredictor
from ....data.loaders.editorial_loader import EditorialLoader
from .model import ContentRecommendationModel


class ContentRecommendationPredictor(BasePredictor):
    """
    Predictor for content recommendation model.
    
    Handles the recommendation workflow:
    1. Get article ID or features
    2. Find similar articles
    3. Format recommendations
    """
    
    def __init__(self, model: Optional[ContentRecommendationModel] = None):
        """
        Initialize content recommendation predictor.
        
        Args:
            model: Content recommendation model instance (if None, creates default)
        """
        if model is None:
            model = ContentRecommendationModel()
        
        super().__init__(model, None)
        self.logger.info("Initialized content recommendation predictor")
    
    def recommend(
        self,
        article_id: str,
        top_n: int = 10,
        **kwargs
    ) -> pd.DataFrame:
        """
        Recommend similar articles based on article_id.
        
        Args:
            article_id: Source article ID
            top_n: Number of recommendations
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with recommended articles
        """
        recommendations = self.model.recommend(
            article_id=article_id,
            top_n=top_n,
            **kwargs
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(recommendations)
        
        # Sort by similarity score
        df = df.sort_values('similarity_score', ascending=False)
        
        return df
    
    def recommend_by_features(
        self,
        features: Dict[str, Any],
        top_n: int = 10,
        **kwargs
    ) -> pd.DataFrame:
        """
        Recommend articles based on feature vector.
        
        Args:
            features: Feature dictionary
            top_n: Number of recommendations
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with recommended articles
        """
        recommendations = self.model.recommend_by_features(
            features=features,
            top_n=top_n,
            **kwargs
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(recommendations)
        
        # Sort by similarity score
        df = df.sort_values('similarity_score', ascending=False)
        
        return df
    
    def recommend_for_user(
        self,
        user_id: str,
        top_n: int = 10,
        **kwargs
    ) -> pd.DataFrame:
        """
        Recommend articles for a user based on their reading history.
        
        Args:
            user_id: User ID
            top_n: Number of recommendations
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with recommended articles
        """
        loader = EditorialLoader()
        
        # Load user's content events to find their preferred articles
        events = loader.load_content_events(
            user_id=user_id,
            limit=100
        )
        
        if len(events) == 0:
            self.logger.warning(f"No content events found for user {user_id}")
            return pd.DataFrame()
        
        # Get user's most engaged articles
        article_engagement = events.groupby('article_id').size().sort_values(ascending=False)
        top_articles = article_engagement.head(5).index.tolist()
        
        # Get recommendations for each top article
        all_recommendations = []
        for article_id in top_articles:
            try:
                recs = self.recommend(article_id, top_n=top_n // len(top_articles) + 1, **kwargs)
                all_recommendations.append(recs)
            except Exception as e:
                self.logger.warning(f"Could not get recommendations for article {article_id}: {e}")
        
        if len(all_recommendations) == 0:
            return pd.DataFrame()
        
        # Combine and deduplicate
        combined = pd.concat(all_recommendations, ignore_index=True)
        combined = combined.drop_duplicates(subset=['article_id'], keep='first')
        
        # Sort by similarity score and return top N
        combined = combined.sort_values('similarity_score', ascending=False).head(top_n)
        
        return combined

