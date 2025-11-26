"""
OwnLens - ML Module: Behavioral Features

Behavioral feature engineering for customer domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class BehavioralFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer behavioral features from user data.
    
    Behavioral features include:
    - Session patterns
    - Event patterns
    - Page navigation patterns
    - Category diversity
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize behavioral feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized behavioral feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer behavioral features.
        
        Args:
            data: User features DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with behavioral features
        """
        data = data.copy()
        
        # Session frequency features
        if 'total_sessions' in data.columns and 'days_active' in data.columns:
            data['sessions_per_day'] = data['total_sessions'] / (data['days_active'] + 1)
            data['avg_days_between_sessions'] = data['days_active'] / (data['total_sessions'] + 1)
        
        # Event intensity features
        if 'avg_events_per_session' in data.columns:
            data['event_intensity'] = data['avg_events_per_session']
            data['high_event_intensity'] = (data['avg_events_per_session'] > data['avg_events_per_session'].quantile(0.75)).astype(int)
        
        # Page navigation features
        if 'avg_pages_per_session' in data.columns and 'avg_unique_pages_count' in data.columns:
            data['page_diversity'] = data['avg_unique_pages_count'] / (data['avg_pages_per_session'] + 1)
        
        # Category diversity features
        if 'avg_categories_diversity' in data.columns:
            data['category_diversity_score'] = data['avg_categories_diversity']
            data['high_category_diversity'] = (data['avg_categories_diversity'] > data['avg_categories_diversity'].quantile(0.75)).astype(int)
        
        # Session duration patterns
        if 'avg_session_duration_sec' in data.columns:
            data['session_duration_minutes'] = data['avg_session_duration_sec'] / 60
            data['long_session_user'] = (data['avg_session_duration_sec'] > 600).astype(int)  # > 10 minutes
        
        # Consistency features
        if 'std_session_duration_sec' in data.columns and 'avg_session_duration_sec' in data.columns:
            data['session_duration_cv'] = data['std_session_duration_sec'] / (data['avg_session_duration_sec'] + 1)
            data['consistent_user'] = (data['session_duration_cv'] < 0.5).astype(int)
        
        return data

