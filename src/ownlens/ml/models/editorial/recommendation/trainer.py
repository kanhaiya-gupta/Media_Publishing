"""
OwnLens - ML Module: Content Recommendation Trainer

Trainer for content recommendation model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd

from ....base.trainer import BaseTrainer
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.content_features import ContentFeatureEngineer
from ....features.editorial.performance_features import PerformanceFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import ContentRecommendationModel


class ContentRecommendationTrainer(BaseTrainer):
    """
    Trainer for content recommendation model.
    
    Handles the complete training workflow:
    1. Load article performance data
    2. Prepare features
    3. Train recommendation model
    """
    
    def __init__(
        self,
        model: Optional[ContentRecommendationModel] = None,
        use_feature_engineers: bool = True
    ):
        """
        Initialize content recommendation trainer.
        
        Args:
            model: Content recommendation model instance (if None, creates default)
            use_feature_engineers: If True, use feature engineering modules
        """
        if model is None:
            model = ContentRecommendationModel()
        
        # Initialize feature engineers
        feature_engineer = None
        if use_feature_engineers:
            content_fe = ContentFeatureEngineer()
            performance_fe = PerformanceFeatureEngineer()
            feature_engineer = CompositeFeatureEngineer([content_fe, performance_fe])
        
        super().__init__(model, feature_engineer, None)
        self.logger.info("Initialized content recommendation trainer")
    
    def load_data(
        self,
        performance_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        min_engagement_score: float = 0.0,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load article performance data for recommendations.
        
        Args:
            performance_date: Performance date (default: today)
            brand_id: Filter by brand_id
            limit: Limit number of articles
            min_engagement_score: Minimum engagement score to include
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with article performance data
        """
        loader = EditorialLoader()
        
        # Load article performance
        performance = loader.load_article_performance(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit
        )
        
        if len(performance) == 0:
            self.logger.warning("No article performance data found")
            return pd.DataFrame()
        
        # Filter by minimum engagement score
        if 'engagement_score' in performance.columns:
            performance = performance[performance['engagement_score'] >= min_engagement_score]
        
        # Load article metadata
        article_ids = performance['article_id'].unique().tolist()
        metadata = loader.load_articles_metadata(
            brand_id=brand_id,
            limit=len(article_ids)
        )
        
        if len(metadata) > 0:
            # Merge with performance data
            data = performance.merge(
                metadata,
                on='article_id',
                how='left'
            )
        else:
            data = performance
        
        self.logger.info(f"Loaded {len(data)} articles for recommendations")
        return data
    
    def prepare_features(
        self,
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for recommendation model.
        
        Args:
            data: Article performance DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with prepared features
        """
        self.logger.info("Preparing features for recommendation")
        
        # Apply feature engineering if available
        if self.feature_engineer:
            self.logger.info("Applying feature engineering...")
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Ensure article_id is present
        if 'article_id' not in data.columns:
            raise ValueError("Article ID column is required for recommendations")
        
        self.logger.info(f"Prepared features for {len(data)} articles")
        return data
    
    def train(
        self,
        performance_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        min_engagement_score: float = 0.0,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            performance_date: Performance date (default: today)
            brand_id: Filter by brand_id
            limit: Limit number of articles
            min_engagement_score: Minimum engagement score to include
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        # Load data
        data = self.load_data(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit,
            min_engagement_score=min_engagement_score
        )
        
        if len(data) == 0:
            raise ValueError("No data loaded for training")
        
        # Prepare features
        data = self.prepare_features(data, **kwargs)
        
        # Train model
        training_metrics = self.model.train(
            data,
            y=None,
            validation_data=None,
            **kwargs
        )
        
        return training_metrics

