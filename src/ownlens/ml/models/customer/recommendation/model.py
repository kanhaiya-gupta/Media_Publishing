"""
OwnLens - ML Module: User Recommendation Model

Content recommendation model for users using collaborative filtering.
"""

from typing import Any, Dict, List
import pandas as pd
import numpy as np
from sklearn.decomposition import NMF
from sklearn.preprocessing import StandardScaler

from ....base.model import BaseMLModel


class UserRecommendationModel(BaseMLModel):
    """
    User content recommendation model using Non-Negative Matrix Factorization (NMF).
    
    Recommends articles to users based on:
    - User-article interaction history
    - Content similarity
    - Collaborative filtering
    """
    
    def __init__(
        self,
        model_id: str = "user_recommendation_v1.0.0",
        model_config: Dict[str, Any] = None,
        model_name: str = "User Content Recommendation Model",
        model_version: str = "1.0.0"
    ):
        """
        Initialize user recommendation model.
        
        Args:
            model_id: Unique model identifier
            model_config: NMF hyperparameters
            model_name: Human-readable model name
            model_version: Model version
        """
        if model_config is None:
            model_config = {
                'n_components': 50,
                'init': 'random',
                'random_state': 42,
                'max_iter': 200,
                'alpha': 0.0,
                'l1_ratio': 0.0
            }
        
        super().__init__(model_id, model_config, model_name, model_version)
        self.user_ids = None
        self.article_ids = None
        self.user_article_matrix = None
    
    def build_model(self) -> NMF:
        """Build NMF user recommendation model."""
        self.model = NMF(**self.model_config)
        self.logger.info("Built NMF user recommendation model")
        return self.model
    
    def train(
        self,
        user_article_interactions: pd.DataFrame,
        y: pd.Series = None,
        validation_data: tuple = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the user recommendation model.
        
        Args:
            user_article_interactions: DataFrame with columns ['user_id', 'article_id', 'interaction_score']
            y: Not used (for compatibility)
            validation_data: Not used (for compatibility)
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        if self.model is None:
            self.build_model()
        
        self.logger.info(f"Training user recommendation model on {len(user_article_interactions)} interactions")
        
        # Create user-article interaction matrix
        self.user_ids = user_article_interactions['user_id'].unique()
        self.article_ids = user_article_interactions['article_id'].unique()
        
        # Create pivot table
        self.user_article_matrix = user_article_interactions.pivot_table(
            index='user_id',
            columns='article_id',
            values='interaction_score',
            fill_value=0
        )
        
        # Ensure all users and articles are in the matrix
        self.user_article_matrix = self.user_article_matrix.reindex(
            index=self.user_ids,
            columns=self.article_ids,
            fill_value=0
        )
        
        # Train NMF model
        W = self.model.fit_transform(self.user_article_matrix.values)
        H = self.model.components_
        
        self.is_trained = True
        
        training_metrics = {
            'n_users': len(self.user_ids),
            'n_articles': len(self.article_ids),
            'n_interactions': len(user_article_interactions),
            'n_components': self.model_config.get('n_components', 50),
            'reconstruction_error': float(self.model.reconstruction_err_)
        }
        
        self.logger.info("User recommendation model training completed")
        return training_metrics
    
    def predict(self, X, **kwargs) -> Any:
        """
        Make predictions (required by BaseMLModel).
        
        For recommendation models, X should be a list/array of user_ids.
        Returns a DataFrame with recommendations for each user.
        
        Args:
            X: User IDs (list, array, or DataFrame with 'user_id' column)
            **kwargs: Additional parameters (top_n, exclude_interacted, etc.)
        
        Returns:
            DataFrame with recommendations
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Handle different input types
        if isinstance(X, pd.DataFrame):
            if 'user_id' in X.columns:
                user_ids = X['user_id'].tolist()
            else:
                raise ValueError("DataFrame must have 'user_id' column")
        elif isinstance(X, (list, np.ndarray)):
            user_ids = list(X)
        elif isinstance(X, str):
            # Single user ID as string
            user_ids = [X]
        else:
            raise ValueError(f"Unsupported input type: {type(X)}")
        
        top_n = kwargs.get('top_n', 10)
        exclude_interacted = kwargs.get('exclude_interacted', True)
        
        return self.recommend_batch(user_ids, top_n=top_n, exclude_interacted=exclude_interacted)
    
    def recommend(
        self,
        user_id: str,
        top_n: int = 10,
        exclude_interacted: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Recommend articles for a user.
        
        Args:
            user_id: User ID
            top_n: Number of recommendations
            exclude_interacted: If True, exclude articles user has already interacted with
            **kwargs: Additional parameters
        
        Returns:
            List of recommended articles with scores
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before recommendation")
        
        # Find user index
        # Convert to list for membership check if it's a numpy array
        user_ids_list = list(self.user_ids) if hasattr(self.user_ids, '__iter__') else [self.user_ids]
        if user_id not in user_ids_list:
            raise ValueError(f"User {user_id} not found in training data")
        
        user_idx = np.where(self.user_ids == user_id)[0][0]
        
        # Get user's latent factors
        W = self.model.transform(self.user_article_matrix.values)
        H = self.model.components_
        
        # Predict ratings for all articles
        user_ratings = np.dot(W[user_idx], H)
        
        # Get article indices sorted by predicted rating
        article_indices = np.argsort(user_ratings)[::-1]
        
        # Exclude already interacted articles if requested
        if exclude_interacted:
            user_interactions = self.user_article_matrix.iloc[user_idx]
            interacted_articles = user_interactions[user_interactions > 0].index
            interacted_indices = [np.where(self.article_ids == aid)[0][0] for aid in interacted_articles if aid in self.article_ids]
            article_indices = [idx for idx in article_indices if idx not in interacted_indices]
        
        # Get top N recommendations
        top_indices = article_indices[:top_n]
        
        # Build recommendations
        recommendations = []
        for idx in top_indices:
            article_id = self.article_ids[idx]
            score = float(user_ratings[idx])
            
            recommendations.append({
                'article_id': article_id,
                'recommendation_score': score
            })
        
        return recommendations
    
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
        all_recommendations = []
        
        for user_id in user_ids:
            try:
                recs = self.recommend(user_id, top_n=top_n, **kwargs)
                for rec in recs:
                    rec['user_id'] = user_id
                    all_recommendations.append(rec)
            except Exception as e:
                self.logger.warning(f"Could not get recommendations for user {user_id}: {e}")
        
        if len(all_recommendations) == 0:
            return pd.DataFrame()
        
        return pd.DataFrame(all_recommendations)

