"""
OwnLens - ML Module: User Recommendation Trainer

Trainer for user content recommendation model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd
import numpy as np

from ....base.trainer import BaseTrainer
from ....data.loaders.editorial_loader import EditorialLoader
from .model import UserRecommendationModel


class UserRecommendationTrainer(BaseTrainer):
    """
    Trainer for user content recommendation model.
    
    Handles the complete training workflow:
    1. Load user-article interactions from events
    2. Prepare interaction matrix
    3. Train NMF model
    """
    
    def __init__(
        self,
        model: Optional[UserRecommendationModel] = None,
        n_components: int = 50
    ):
        """
        Initialize user recommendation trainer.
        
        Args:
            model: User recommendation model instance (if None, creates default)
            n_components: Number of latent factors for NMF
        """
        if model is None:
            model_config = {'n_components': n_components}
            model = UserRecommendationModel(model_config=model_config)
        
        super().__init__(model, None, None)
        self.logger.info("Initialized user recommendation trainer")
    
    def load_data(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load user-article interactions from events.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of events
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with user-article interactions
        """
        # Load content events (user-article interactions)
        editorial_loader = EditorialLoader()
        events = editorial_loader.load_content_events(
            start_date=start_date,
            end_date=end_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit
        )
        
        if len(events) == 0:
            self.logger.warning("No content events found")
            return pd.DataFrame()
        
        # Filter to events with user_id and article_id
        events = events[events['user_id'].notna() & events['article_id'].notna()]
        
        if len(events) == 0:
            self.logger.warning("No events with user_id and article_id found")
            return pd.DataFrame()
        
        # Create interaction scores based on event types
        interaction_scores = {
            'article_view': 1.0,
            'article_click': 3.0,
            'article_share': 5.0,
            'article_like': 4.0,
            'article_bookmark': 4.0
        }
        
        # Aggregate interactions per user-article pair
        interactions = []
        
        for (user_id, article_id), group in events.groupby(['user_id', 'article_id']):
            # Calculate interaction score
            score = 0.0
            for event_type in group['event_type'].values:
                score += interaction_scores.get(event_type, 1.0)
            
            # Normalize by number of interactions
            score = score / len(group)
            
            interactions.append({
                'user_id': user_id,
                'article_id': article_id,
                'interaction_score': score,
                'interaction_count': len(group)
            })
        
        interactions_df = pd.DataFrame(interactions)
        
        self.logger.info(f"Loaded {len(interactions_df)} user-article interactions")
        return interactions_df
    
    def prepare_features(
        self,
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare user-article interaction matrix.
        
        Args:
            data: User-article interactions DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with interactions (ready for training)
        """
        self.logger.info("Preparing user-article interaction matrix")
        
        # Ensure required columns exist
        required_cols = ['user_id', 'article_id', 'interaction_score']
        missing_cols = [col for col in required_cols if col not in data.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Aggregate duplicate user-article pairs
        interactions = data.groupby(['user_id', 'article_id']).agg({
            'interaction_score': 'mean',
            'interaction_count': 'sum' if 'interaction_count' in data.columns else 'count'
        }).reset_index()
        
        self.logger.info(f"Prepared {len(interactions)} unique user-article interactions")
        return interactions
    
    def train(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of events
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        # Load data
        data = self.load_data(
            start_date=start_date,
            end_date=end_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit
        )
        
        if len(data) == 0:
            raise ValueError("No data loaded for training")
        
        # Prepare features
        interactions = self.prepare_features(data, **kwargs)
        
        # Train model
        training_metrics = self.model.train(
            interactions,
            y=None,
            validation_data=None,
            **kwargs
        )
        
        return training_metrics

