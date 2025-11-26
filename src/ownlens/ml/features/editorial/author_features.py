"""
OwnLens - ML Module: Author Features

Author feature engineering for editorial domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class AuthorFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer author features from author performance data.
    
    Author features include:
    - Publishing frequency features
    - Performance features
    - Engagement features
    - Ranking features
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize author feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized author feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer author features.
        
        Args:
            data: Author performance data DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with author features
        """
        data = data.copy()
        
        # Publishing frequency features
        if 'articles_published' in data.columns:
            data['publishing_frequency'] = data['articles_published']
            data['high_frequency_author'] = (data['articles_published'] > data['articles_published'].quantile(0.75)).astype(int)
            data['prolific_author'] = (data['articles_published'] > data['articles_published'].quantile(0.9)).astype(int)
        
        # Performance features
        if 'avg_views_per_article' in data.columns:
            data['views_per_article'] = data['avg_views_per_article']
            data['high_performing_author'] = (data['avg_views_per_article'] > data['avg_views_per_article'].quantile(0.75)).astype(int)
        
        # Engagement features
        if 'avg_engagement_score' in data.columns:
            data['author_engagement'] = data['avg_engagement_score']
            data['high_engagement_author'] = (data['avg_engagement_score'] > data['avg_engagement_score'].quantile(0.75)).astype(int)
        
        # Publishing efficiency (views per published article)
        if 'total_views' in data.columns and 'articles_published' in data.columns:
            data['views_per_published_article'] = data['total_views'] / (data['articles_published'] + 1)
            data['efficient_author'] = (data['views_per_published_article'] > data['views_per_published_article'].quantile(0.75)).astype(int)
        
        # Ranking features
        if 'author_rank' in data.columns:
            data['top_ranked_author'] = (data['author_rank'] <= 10).astype(int)
            data['top_10_author'] = (data['author_rank'] <= 10).astype(int)
            data['top_5_author'] = (data['author_rank'] <= 5).astype(int)
        
        if 'engagement_rank' in data.columns:
            data['top_engagement_author'] = (data['engagement_rank'] <= 10).astype(int)
        
        # Best article features
        if 'best_article_views' in data.columns:
            data['has_best_article'] = (data['best_article_views'] > 0).astype(int)
            data['best_article_performance'] = data['best_article_views'].fillna(0)
        
        # Category specialization
        if 'top_category_views' in data.columns:
            data['category_specialist'] = (data['top_category_views'] > 0).astype(int)
        
        # Author performance score (composite)
        performance_components = []
        
        if 'avg_engagement_score' in data.columns:
            # Normalize engagement score
            max_engagement = data['avg_engagement_score'].max()
            if max_engagement > 0:
                performance_components.append((data['avg_engagement_score'] / max_engagement) * 3)
        
        if 'avg_views_per_article' in data.columns:
            # Normalize views per article
            max_views = data['avg_views_per_article'].max()
            if max_views > 0:
                performance_components.append((data['avg_views_per_article'] / max_views) * 2)
        
        if 'articles_published' in data.columns:
            # Normalize publishing frequency
            max_published = data['articles_published'].max()
            if max_published > 0:
                performance_components.append((data['articles_published'] / max_published) * 1)
        
        if performance_components:
            data['author_performance_score'] = pd.concat(performance_components, axis=1).sum(axis=1) / len(performance_components)
        else:
            data['author_performance_score'] = 0.0
        
        return data

