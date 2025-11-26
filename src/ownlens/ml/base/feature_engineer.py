"""
OwnLens - ML Module: Base Feature Engineer

Base class for feature engineers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BaseFeatureEngineer(ABC):
    """
    Base class for feature engineers.
    
    Handles feature engineering including:
    - Feature extraction
    - Feature transformation
    - Feature selection
    """
    
    def __init__(self, feature_list: Optional[List[str]] = None):
        """
        Initialize the feature engineer.
        
        Args:
            feature_list: List of feature names to use
        """
        self.feature_list = feature_list
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def engineer_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Engineer features from raw data.
        
        Args:
            data: Raw data
            **kwargs: Feature engineering parameters
        
        Returns:
            DataFrame with engineered features
        """
        pass
    
    def select_features(
        self,
        data: pd.DataFrame,
        feature_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Select features from data.
        
        Args:
            data: DataFrame with features
            feature_list: List of feature names to select
        
        Returns:
            DataFrame with selected features
        """
        feature_list = feature_list or self.feature_list
        
        if feature_list is None:
            return data
        
        # Select only features that exist in data
        available_features = [f for f in feature_list if f in data.columns]
        missing_features = [f for f in feature_list if f not in data.columns]
        
        if missing_features:
            self.logger.warning(
                f"Missing features: {missing_features}"
            )
        
        return data[available_features]
    
    def handle_missing_values(
        self,
        data: pd.DataFrame,
        strategy: str = 'fill_zero'
    ) -> pd.DataFrame:
        """
        Handle missing values in features.
        
        Args:
            data: DataFrame with features
            strategy: Strategy for handling missing values
                - 'fill_zero': Fill with 0
                - 'fill_mean': Fill with mean
                - 'fill_median': Fill with median
                - 'drop': Drop rows with missing values
        
        Returns:
            DataFrame with handled missing values
        """
        if strategy == 'fill_zero':
            return data.fillna(0)
        elif strategy == 'fill_mean':
            return data.fillna(data.mean())
        elif strategy == 'fill_median':
            return data.fillna(data.median())
        elif strategy == 'drop':
            return data.dropna()
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

