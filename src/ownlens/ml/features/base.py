"""
OwnLens - ML Module: Base Feature Engineer

Base class for feature engineers.
"""

from typing import List, Optional
import pandas as pd
import logging

from ..base.feature_engineer import BaseFeatureEngineer as BaseFE

logger = logging.getLogger(__name__)


class BaseFeatureEngineer(BaseFE):
    """
    Base feature engineer for ownlens ML module.
    
    Extends the base class with ownlens-specific utilities.
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """
        Initialize feature engineer.
        
        Args:
            feature_list: List of feature names to use
        """
        super().__init__(feature_list)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer features from raw data.
        
        This is the main method that should be overridden by subclasses.
        
        Args:
            data: Raw data
            **kwargs: Feature engineering parameters
        
        Returns:
            DataFrame with engineered features
        """
        # Default implementation: just return data
        return data

