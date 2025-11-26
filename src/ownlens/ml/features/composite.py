"""
OwnLens - ML Module: Composite Feature Engineer

Composite feature engineer that chains multiple feature engineers.
"""

from typing import List
import pandas as pd
import logging

from .base import BaseFeatureEngineer

logger = logging.getLogger(__name__)


class CompositeFeatureEngineer(BaseFeatureEngineer):
    """
    Composite feature engineer that chains multiple feature engineers.
    
    Applies feature engineers in sequence to build up features.
    """
    
    def __init__(self, feature_engineers: List[BaseFeatureEngineer]):
        """
        Initialize composite feature engineer.
        
        Args:
            feature_engineers: List of feature engineers to apply in sequence
        """
        super().__init__()
        self.feature_engineers = feature_engineers
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info(f"Initialized composite feature engineer with {len(feature_engineers)} engineers")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Apply all feature engineers in sequence.
        
        Args:
            data: Input data
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with all engineered features
        """
        result = data.copy()
        
        for i, fe in enumerate(self.feature_engineers):
            self.logger.debug(f"Applying feature engineer {i+1}/{len(self.feature_engineers)}: {fe.__class__.__name__}")
            result = fe.engineer_features(result, **kwargs)
        
        self.logger.info(f"Applied {len(self.feature_engineers)} feature engineers, final shape: {result.shape}")
        return result





