"""
OwnLens - ML Module: Performance Features

Performance feature engineering for editorial domain.
"""

from typing import Optional, List
import pandas as pd
import numpy as np
import logging

from ..base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class PerformanceFeatureEngineer(BaseFeatureEngineer):
    """
    Engineer performance features from article/author/category performance data.
    
    Performance features include:
    - Engagement rate features
    - Conversion features
    - Growth features
    - Trending features
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """Initialize performance feature engineer."""
        super().__init__(feature_list)
        self.logger.info("Initialized performance feature engineer")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer performance features.
        
        Args:
            data: Performance data DataFrame
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with performance features
        """
        data = data.copy()
        
        # Engagement rate features
        if 'total_views' in data.columns and 'total_clicks' in data.columns:
            data['click_through_rate'] = data['total_clicks'] / (data['total_views'] + 1)
            data['high_ctr'] = (data['click_through_rate'] > data['click_through_rate'].quantile(0.75)).astype(int)
        
        if 'total_views' in data.columns and 'total_shares' in data.columns:
            data['share_rate'] = data['total_shares'] / (data['total_views'] + 1)
            data['viral_content'] = (data['share_rate'] > data['share_rate'].quantile(0.9)).astype(int)
        
        # Reading quality features
        if 'completion_rate' in data.columns:
            data['high_completion'] = (data['completion_rate'] > 0.5).astype(int)
            data['excellent_completion'] = (data['completion_rate'] > 0.75).astype(int)
        
        if 'bounce_rate' in data.columns:
            data['low_bounce'] = (data['bounce_rate'] < 0.5).astype(int)
            data['high_bounce'] = (data['bounce_rate'] > 0.7).astype(int)
        
        # Video engagement features
        if 'video_plays' in data.columns and 'video_completions' in data.columns:
            data['video_completion_rate'] = data['video_completions'] / (data['video_plays'] + 1)
            data['high_video_engagement'] = (data['video_completion_rate'] > 0.5).astype(int)
        
        # Referral source features
        if 'total_views' in data.columns:
            referral_cols = ['direct_views', 'search_views', 'social_views', 'newsletter_views', 'internal_views']
            available_refs = [col for col in referral_cols if col in data.columns]
            
            for ref_col in available_refs:
                data[f'{ref_col}_rate'] = data[ref_col] / (data['total_views'] + 1)
            
            # Dominant referral source
            if available_refs:
                ref_rates = [f'{col}_rate' for col in available_refs]
                data['dominant_referral'] = data[ref_rates].idxmax(axis=1).str.replace('_rate', '')
        
        # Device distribution features
        if 'total_views' in data.columns:
            device_cols = ['desktop_views', 'mobile_views', 'tablet_views']
            available_devices = [col for col in device_cols if col in data.columns]
            
            for device_col in available_devices:
                data[f'{device_col}_rate'] = data[device_col] / (data['total_views'] + 1)
            
            # Mobile-first content
            if 'mobile_views_rate' in data.columns:
                data['mobile_first'] = (data['mobile_views_rate'] > 0.6).astype(int)
        
        # Growth features
        if 'engagement_growth_rate' in data.columns:
            data['growing_engagement'] = (data['engagement_growth_rate'] > 0).astype(int)
            data['declining_engagement'] = (data['engagement_growth_rate'] < 0).astype(int)
            data['strong_growth'] = (data['engagement_growth_rate'] > 0.1).astype(int)
        
        # Trending features
        if 'is_trending' in data.columns:
            data['trending'] = data['is_trending']
        
        if 'trending_score' in data.columns:
            data['trending_score_normalized'] = data['trending_score'].fillna(0)
            data['high_trending'] = (data['trending_score'] > data['trending_score'].quantile(0.75)).astype(int)
        
        if 'trending_rank' in data.columns:
            data['top_trending'] = (data['trending_rank'] <= 10).astype(int)
            data['top_5_trending'] = (data['trending_rank'] <= 5).astype(int)
        
        # Peak time features
        if 'peak_hour' in data.columns:
            data['peak_morning'] = ((data['peak_hour'] >= 6) & (data['peak_hour'] < 12)).astype(int)
            data['peak_afternoon'] = ((data['peak_hour'] >= 12) & (data['peak_hour'] < 18)).astype(int)
            data['peak_evening'] = ((data['peak_hour'] >= 18) | (data['peak_hour'] < 6)).astype(int)
        
        if 'peak_day_of_week' in data.columns:
            data['peak_weekend'] = (data['peak_day_of_week'] >= 5).astype(int)
            data['peak_weekday'] = (data['peak_day_of_week'] < 5).astype(int)
        
        # Revenue features
        if 'ad_revenue' in data.columns:
            data['has_revenue'] = (data['ad_revenue'] > 0).astype(int)
            data['high_revenue'] = (data['ad_revenue'] > data['ad_revenue'].quantile(0.75)).astype(int)
        
        if 'subscription_conversions' in data.columns:
            data['has_conversions'] = (data['subscription_conversions'] > 0).astype(int)
            data['high_conversions'] = (data['subscription_conversions'] > data['subscription_conversions'].quantile(0.75)).astype(int)
        
        # Overall performance score (composite)
        performance_components = []
        
        if 'engagement_score' in data.columns:
            max_engagement = data['engagement_score'].max()
            if max_engagement > 0:
                performance_components.append((data['engagement_score'] / max_engagement) * 3)
        
        if 'click_through_rate' in data.columns:
            max_ctr = data['click_through_rate'].max()
            if max_ctr > 0:
                performance_components.append((data['click_through_rate'] / max_ctr) * 2)
        
        if 'completion_rate' in data.columns:
            performance_components.append(data['completion_rate'] * 2)
        
        if 'trending_score' in data.columns:
            max_trending = data['trending_score'].fillna(0).max()
            if max_trending > 0:
                performance_components.append((data['trending_score'].fillna(0) / max_trending) * 1)
        
        if performance_components:
            data['overall_performance_score'] = pd.concat(performance_components, axis=1).sum(axis=1) / len(performance_components)
        else:
            data['overall_performance_score'] = 0.0
        
        return data

