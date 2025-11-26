"""
OwnLens - ML Module: Content Features

Content feature engineering for editorial domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class ContentFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer content features from article data.
    
    Content features include:
    - Article metadata features
    - Content quality features
    - Media features
    - Publishing features
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize content feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized content feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer content features.
        
        Args:
            data: Article data DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with content features
        """
        data = data.copy()
        
        # Article length features
        if 'word_count' in data.columns:
            data['article_length_category'] = pd.cut(
                data['word_count'],
                bins=[0, 500, 1000, 2000, float('inf')],
                labels=['short', 'medium', 'long', 'very_long']
            )
            data['long_article'] = (data['word_count'] > 2000).astype(int)
            data['short_article'] = (data['word_count'] < 500).astype(int)
        
        # Reading time features
        if 'reading_time_minutes' in data.columns:
            data['quick_read'] = (data['reading_time_minutes'] < 5).astype(int)
            data['long_read'] = (data['reading_time_minutes'] > 10).astype(int)
        
        # Media features
        if 'image_count' in data.columns:
            data['has_images'] = (data['image_count'] > 0).astype(int)
            data['image_rich'] = (data['image_count'] > 5).astype(int)
            data['images_per_1000_words'] = (data['image_count'] / (data['word_count'] / 1000 + 1)).fillna(0)
        
        if 'video_count' in data.columns:
            data['has_videos'] = (data['video_count'] > 0).astype(int)
            data['video_rich'] = (data['video_count'] > 1).astype(int)
        
        # Article type features
        if 'article_type' in data.columns:
            # One-hot encode article types
            article_types = data['article_type'].unique()
            for article_type in article_types:
                if pd.notna(article_type):
                    data[f'is_{article_type}'] = (data['article_type'] == article_type).astype(int)
        
        # Publishing features
        if 'publish_time' in data.columns:
            data['publish_time'] = pd.to_datetime(data['publish_time'])
            data['publish_hour'] = data['publish_time'].dt.hour
            data['publish_day_of_week'] = data['publish_time'].dt.dayofweek
            data['publish_is_weekend'] = (data['publish_day_of_week'] >= 5).astype(int)
            data['publish_is_morning'] = ((data['publish_hour'] >= 6) & (data['publish_hour'] < 12)).astype(int)
            data['publish_is_afternoon'] = ((data['publish_hour'] >= 12) & (data['publish_hour'] < 18)).astype(int)
            data['publish_is_evening'] = ((data['publish_hour'] >= 18) | (data['publish_hour'] < 6)).astype(int)
        
        # Content quality score (composite)
        quality_components = []
        
        if 'word_count' in data.columns:
            # Optimal word count is 1000-2000
            optimal_word_count = ((data['word_count'] >= 1000) & (data['word_count'] <= 2000)).astype(int)
            quality_components.append(optimal_word_count * 2)
        
        if 'image_count' in data.columns:
            # Articles with images score higher
            quality_components.append((data['image_count'] > 0).astype(int) * 1)
        
        if 'video_count' in data.columns:
            # Articles with videos score higher
            quality_components.append((data['video_count'] > 0).astype(int) * 2)
        
        if quality_components:
            data['content_quality_score'] = pd.concat(quality_components, axis=1).sum(axis=1)
        else:
            data['content_quality_score'] = 0
        
        return data

