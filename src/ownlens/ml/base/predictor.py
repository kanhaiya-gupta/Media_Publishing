"""
OwnLens - ML Module: Base Predictor

Base class for model predictors.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BasePredictor(ABC):
    """
    Base class for model predictors.
    
    Handles the prediction workflow including:
    - Model loading
    - Feature preparation
    - Prediction
    - Result formatting
    """
    
    def __init__(
        self,
        model: Any,
        feature_engineer: Optional[Any] = None
    ):
        """
        Initialize the predictor.
        
        Args:
            model: ML model instance
            feature_engineer: Feature engineer instance
        """
        self.model = model
        self.feature_engineer = feature_engineer
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def load_model(self, path: str) -> bool:
        """
        Load model from disk or registry.
        
        Args:
            path: Path to model or model ID
        
        Returns:
            True if successful, False otherwise
        """
        return self.model.load(path)
    
    @abstractmethod
    def prepare_features(self, data: Any, **kwargs) -> Any:
        """
        Prepare features for prediction.
        
        Args:
            data: Raw data
            **kwargs: Feature engineering parameters
        
        Returns:
            Prepared features (X)
        """
        pass
    
    def predict(self, data: Any, **kwargs) -> Any:
        """
        Make predictions.
        
        Args:
            data: Input data
            **kwargs: Prediction parameters
        
        Returns:
            Predictions
        """
        # Prepare features
        X = self.prepare_features(data, **kwargs)
        
        # Make predictions
        predictions = self.model.predict(X, **kwargs)
        
        return predictions
    
    def predict_proba(self, data: Any, **kwargs) -> Any:
        """
        Predict probabilities (for classification models).
        
        Args:
            data: Input data
            **kwargs: Prediction parameters
        
        Returns:
            Prediction probabilities
        """
        # Prepare features
        X = self.prepare_features(data, **kwargs)
        
        # Make predictions
        probabilities = self.model.predict_proba(X, **kwargs)
        
        return probabilities

