"""
OwnLens - ML Module: Engagement Features

Engagement feature engineering for customer domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class EngagementFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer engagement features from user data.
    
    Engagement features include:
    - Content engagement
    - Video engagement
    - Newsletter engagement
    - Overall engagement scores
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize engagement feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized engagement feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer engagement features.
        
        Args:
            data: User features DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with engagement features
        """
        data = data.copy()
        
        # Content engagement rate
        if 'avg_article_views' in data.columns and 'avg_article_clicks' in data.columns:
            data['article_click_rate'] = data['avg_article_clicks'] / (data['avg_article_views'] + 1)
            data['high_click_rate'] = (data['article_click_rate'] > data['article_click_rate'].quantile(0.75)).astype(int)
        
        # Video engagement
        if 'avg_video_plays' in data.columns:
            data['video_engagement'] = data['avg_video_plays']
            data['video_user'] = (data['avg_video_plays'] > 0).astype(int)
        
        # Newsletter engagement
        if 'total_newsletter_signups' in data.columns and 'total_sessions' in data.columns:
            data['newsletter_signup_rate'] = data['total_newsletter_signups'] / (data['total_sessions'] + 1)
            data['newsletter_subscriber'] = (data['total_newsletter_signups'] > 0).astype(int)
        
        # Search engagement
        if 'avg_searches' in data.columns:
            data['search_engagement'] = data['avg_searches']
            data['active_searcher'] = (data['avg_searches'] > data['avg_searches'].quantile(0.75)).astype(int)
        
        # Overall engagement score (use pre-computed if available, otherwise calculate)
        if 'avg_engagement_score' in data.columns:
            data['engagement_score'] = data['avg_engagement_score']
        else:
            # Calculate engagement score
            engagement_components = []
            
            if 'avg_article_views' in data.columns:
                engagement_components.append(data['avg_article_views'] * 2)
            if 'avg_article_clicks' in data.columns:
                engagement_components.append(data['avg_article_clicks'] * 3)
            if 'avg_video_plays' in data.columns:
                engagement_components.append(data['avg_video_plays'] * 5)
            if 'total_newsletter_signups' in data.columns:
                engagement_components.append(data['total_newsletter_signups'] * 20)
            if 'avg_pages_per_session' in data.columns:
                engagement_components.append(data['avg_pages_per_session'] * 1)
            if 'avg_categories_diversity' in data.columns:
                engagement_components.append(data['avg_categories_diversity'] * 2)
            
            if engagement_components:
                data['engagement_score'] = pd.concat(engagement_components, axis=1).sum(axis=1)
            else:
                data['engagement_score'] = 0.0
        
        # Engagement level categorization
        if 'engagement_score' in data.columns:
            data['engagement_level'] = pd.cut(
                data['engagement_score'],
                bins=[0, 10, 50, 100, float('inf')],
                labels=['low', 'medium', 'high', 'very_high']
            )
            data['high_engagement'] = (data['engagement_score'] > data['engagement_score'].quantile(0.75)).astype(int)
        
        return data

