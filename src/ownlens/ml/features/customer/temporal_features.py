"""
OwnLens - ML Module: Temporal Features

Temporal feature engineering for customer domain.
"""

from typing import Optional, List
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class TemporalFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer temporal features from user data.
    
    Temporal features include:
    - Recency features
    - Frequency features
    - Seasonality features
    - Time-based patterns
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize temporal feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized temporal feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer temporal features.
        
        Args:
            data: User features DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with temporal features
        """
        data = data.copy()
        
        # Recency features (already in customer_user_features, but add derived)
        if 'days_since_last_session' in data.columns:
            data['recency_score'] = 1.0 / (data['days_since_last_session'] + 1)
            data['recent_user'] = (data['days_since_last_session'] <= 7).astype(int)
            data['inactive_user'] = (data['days_since_last_session'] >= 30).astype(int)
            data['churned_user'] = (data['days_since_last_session'] >= 30).astype(int)
        
        # Frequency features
        if 'total_sessions' in data.columns:
            data['session_frequency'] = data['total_sessions']
            data['frequent_user'] = (data['total_sessions'] > data['total_sessions'].quantile(0.75)).astype(int)
            data['power_user'] = (data['total_sessions'] > data['total_sessions'].quantile(0.9)).astype(int)
        
        # Activity recency features
        if 'active_today' in data.columns:
            data['very_recent'] = data['active_today']
        if 'active_last_7_days' in data.columns:
            data['recent_activity'] = data['active_last_7_days']
        if 'active_last_30_days' in data.columns:
            data['monthly_active'] = data['active_last_30_days']
        
        # Tenure features
        if 'days_active' in data.columns:
            data['user_tenure_days'] = data['days_active']
            data['new_user'] = (data['days_active'] <= 7).astype(int)
            data['established_user'] = (data['days_active'] >= 30).astype(int)
            data['long_term_user'] = (data['days_active'] >= 90).astype(int)
        
        # First/last session features
        if 'first_session_date' in data.columns and 'last_session_date' in data.columns:
            # Calculate user lifetime
            data['user_lifetime_days'] = (
                pd.to_datetime(data['last_session_date']) - 
                pd.to_datetime(data['first_session_date'])
            ).dt.days
            data['user_lifetime_days'] = data['user_lifetime_days'].fillna(0)
        
        # Session frequency over time
        if 'total_sessions' in data.columns and 'days_active' in data.columns:
            data['sessions_per_week'] = (data['total_sessions'] / ((data['days_active'] + 1) / 7))
            data['sessions_per_week'] = data['sessions_per_week'].fillna(0)
        
        return data

