"""
OwnLens - ML Module: Churn Features

Churn-specific feature engineering for customer domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class ChurnFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer churn-specific features.
    
    Churn features include:
    - Churn risk indicators
    - Engagement decline indicators
    - Activity decline indicators
    - Churn probability features
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize churn feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized churn feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer churn-specific features.
        
        Args:
            data: User features DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with churn features
        """
        data = data.copy()
        
        # Churn risk indicators
        if 'days_since_last_session' in data.columns:
            data['churn_risk_days'] = data['days_since_last_session']
            data['churn_risk_high'] = (data['days_since_last_session'] >= 30).astype(int)
            data['churn_risk_critical'] = (data['days_since_last_session'] >= 60).astype(int)
        
        # Engagement decline indicators
        if 'avg_engagement_score' in data.columns:
            # Compare to median engagement
            median_engagement = data['avg_engagement_score'].median()
            data['engagement_below_median'] = (data['avg_engagement_score'] < median_engagement).astype(int)
            data['low_engagement'] = (data['avg_engagement_score'] < data['avg_engagement_score'].quantile(0.25)).astype(int)
        
        # Activity decline indicators
        if 'total_sessions' in data.columns and 'days_active' in data.columns:
            # Sessions per day trend
            data['sessions_per_day'] = data['total_sessions'] / (data['days_active'] + 1)
            data['low_activity'] = (data['sessions_per_day'] < data['sessions_per_day'].quantile(0.25)).astype(int)
        
        # Subscription tier and churn risk
        if 'current_subscription_tier' in data.columns:
            # Free tier users more likely to churn
            data['free_tier_user'] = (data['current_subscription_tier'] == 'free').astype(int)
            data['premium_tier_user'] = (data['current_subscription_tier'].isin(['premium', 'pro', 'enterprise'])).astype(int)
        
        # Newsletter subscription and churn risk
        if 'has_newsletter' in data.columns:
            data['newsletter_subscriber'] = data['has_newsletter']
            # Newsletter subscribers less likely to churn
        
        # Use pre-computed churn features if available
        if 'churn_risk_score' in data.columns:
            data['churn_risk'] = data['churn_risk_score']
        if 'churn_probability' in data.columns:
            data['churn_prob'] = data['churn_probability']
        
        # Combined churn risk score
        risk_components = []
        
        if 'days_since_last_session' in data.columns:
            risk_components.append(data['days_since_last_session'] / 30.0)  # Normalize to 30 days
        
        if 'avg_engagement_score' in data.columns:
            median_eng = data['avg_engagement_score'].median()
            if median_eng > 0:
                risk_components.append(1.0 - (data['avg_engagement_score'] / median_eng).clip(0, 1))
        
        if 'has_newsletter' in data.columns:
            risk_components.append((1 - data['has_newsletter']) * 0.2)  # Newsletter reduces risk
        
        if risk_components:
            data['combined_churn_risk'] = pd.concat(risk_components, axis=1).sum(axis=1) / len(risk_components)
        else:
            data['combined_churn_risk'] = 0.0
        
        return data

