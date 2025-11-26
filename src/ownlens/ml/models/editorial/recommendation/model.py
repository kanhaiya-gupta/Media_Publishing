"""
OwnLens - ML Module: Content Recommendation Model

Content recommendation model based on article performance.
"""

from typing import Any, Dict, List
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler

from ....base.model import BaseMLModel


class ContentRecommendationModel(BaseMLModel):
    """
    Content recommendation model using collaborative filtering and content-based features.
    
    Recommends articles based on:
    - Article performance metrics
    - Content similarity
    - User engagement patterns
    """
    
    def __init__(
        self,
        model_id: str = "content_recommendation_v1.0.0",
        model_config: Dict[str, Any] = None,
        model_name: str = "Content Recommendation Model",
        model_version: str = "1.0.0"
    ):
        """
        Initialize content recommendation model.
        
        Args:
            model_id: Unique model identifier
            model_config: Model configuration
            model_name: Human-readable model name
            model_version: Model version
        """
        if model_config is None:
            model_config = {
                'similarity_metric': 'cosine',
                'top_n': 10,
                'min_engagement_score': 0.0
            }
        
        super().__init__(model_id, model_config, model_name, model_version)
        self.scaler = StandardScaler()
        self.article_features = None
        self.article_similarity = None
    
    def build_model(self):
        """Build content recommendation model."""
        self.model = self  # Self-reference for compatibility
        self.logger.info("Built content recommendation model")
        return self.model
    
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series = None,
        validation_data: tuple = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the content recommendation model.
        
        Args:
            X: Article features DataFrame (article_id, engagement_score, etc.)
            y: Not used (for compatibility)
            validation_data: Not used (for compatibility)
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        if X is None or len(X) == 0:
            raise ValueError("Article features DataFrame is required")
        
        self.logger.info(f"Training content recommendation model on {len(X)} articles")
        
        # Store article features
        self.article_features = X.copy()
        
        # Select feature columns for similarity calculation
        feature_cols = [
            col for col in X.columns 
            if col not in ['article_id', 'brand_id', 'performance_date', 
                          'created_at', 'updated_at', 'title', 'headline']
        ]
        
        # Scale features
        feature_matrix = X[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
        feature_matrix_scaled = self.scaler.fit_transform(feature_matrix)
        
        # Calculate article similarity matrix
        self.article_similarity = cosine_similarity(feature_matrix_scaled)
        
        # Store article IDs for lookup
        self.article_ids = X['article_id'].values if 'article_id' in X.columns else X.index.values
        
        self.is_trained = True
        
        training_metrics = {
            'n_articles': len(X),
            'n_features': len(feature_cols),
            'similarity_matrix_shape': self.article_similarity.shape
        }
        
        self.logger.info("Content recommendation model training completed")
        return training_metrics
    
    def recommend(
        self,
        article_id: str,
        top_n: int = None,
        exclude_article: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Recommend similar articles based on article_id.
        
        Args:
            article_id: Source article ID
            top_n: Number of recommendations (default: from config)
            exclude_article: If True, exclude source article from recommendations
            **kwargs: Additional parameters
        
        Returns:
            List of recommended articles with similarity scores
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before recommendation")
        
        if top_n is None:
            top_n = self.model_config.get('top_n', 10)
        
        # Find article index
        if 'article_id' in self.article_features.columns:
            article_idx = self.article_features[self.article_features['article_id'] == article_id].index
            if len(article_idx) == 0:
                raise ValueError(f"Article {article_id} not found in training data")
            article_idx = article_idx[0]
        else:
            article_idx = np.where(self.article_ids == article_id)[0]
            if len(article_idx) == 0:
                raise ValueError(f"Article {article_id} not found in training data")
            article_idx = article_idx[0]
        
        # Get similarity scores
        similarities = self.article_similarity[article_idx]
        
        # Get top N similar articles
        top_indices = np.argsort(similarities)[::-1]
        
        if exclude_article:
            top_indices = top_indices[top_indices != article_idx]
        
        top_indices = top_indices[:top_n]
        
        # Build recommendations
        recommendations = []
        for idx in top_indices:
            rec_article_id = self.article_ids[idx]
            similarity_score = float(similarities[idx])
            
            # Get article metadata
            article_data = self.article_features.iloc[idx].to_dict()
            
            recommendations.append({
                'article_id': rec_article_id,
                'similarity_score': similarity_score,
                **article_data
            })
        
        return recommendations
    
    def recommend_by_features(
        self,
        features: Dict[str, Any],
        top_n: int = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Recommend articles based on feature vector.
        
        Args:
            features: Feature dictionary
            top_n: Number of recommendations (default: from config)
            **kwargs: Additional parameters
        
        Returns:
            List of recommended articles with similarity scores
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before recommendation")
        
        if top_n is None:
            top_n = self.model_config.get('top_n', 10)
        
        # Convert features to DataFrame
        feature_df = pd.DataFrame([features])
        
        # Select feature columns (same as training)
        feature_cols = [
            col for col in self.article_features.columns 
            if col not in ['article_id', 'brand_id', 'performance_date', 
                          'created_at', 'updated_at', 'title', 'headline']
        ]
        
        # Scale features
        feature_vector = feature_df[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
        feature_vector_scaled = self.scaler.transform(feature_vector)
        
        # Calculate similarity with all articles
        article_features_scaled = self.scaler.transform(
            self.article_features[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
        )
        similarities = cosine_similarity(feature_vector_scaled, article_features_scaled)[0]
        
        # Get top N similar articles
        top_indices = np.argsort(similarities)[::-1][:top_n]
        
        # Build recommendations
        recommendations = []
        for idx in top_indices:
            rec_article_id = self.article_ids[idx]
            similarity_score = float(similarities[idx])
            
            # Get article metadata
            article_data = self.article_features.iloc[idx].to_dict()
            
            recommendations.append({
                'article_id': rec_article_id,
                'similarity_score': similarity_score,
                **article_data
            })
        
        return recommendations

