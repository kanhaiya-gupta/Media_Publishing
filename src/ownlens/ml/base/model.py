"""
OwnLens - ML Module: Base Model

Base class for all ML models.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BaseMLModel(ABC):
    """
    Base class for all ML models.
    
    All ML models should inherit from this class and implement:
    - build_model(): Build the model architecture
    - train(): Train the model
    - predict(): Make predictions
    """
    
    def __init__(
        self,
        model_id: str,
        model_config: Dict[str, Any],
        model_name: Optional[str] = None,
        model_version: Optional[str] = None
    ):
        """
        Initialize the base ML model.
        
        Args:
            model_id: Unique model identifier
            model_config: Model configuration (hyperparameters, etc.)
            model_name: Human-readable model name
            model_version: Model version (semantic versioning)
        """
        self.model_id = model_id
        self.model_config = model_config
        self.model_name = model_name or model_id
        self.model_version = model_version or "1.0.0"
        self.model = None
        self.is_trained = False
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def build_model(self) -> Any:
        """
        Build the model architecture.
        
        Returns:
            The model object (e.g., XGBoost model, sklearn model, etc.)
        """
        pass
    
    @abstractmethod
    def train(self, X, y, **kwargs) -> Dict[str, Any]:
        """
        Train the model.
        
        Args:
            X: Training features
            y: Training labels
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        pass
    
    @abstractmethod
    def predict(self, X, **kwargs) -> Any:
        """
        Make predictions.
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Predictions
        """
        pass
    
    def predict_proba(self, X, **kwargs) -> Any:
        """
        Predict probabilities (for classification models).
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Prediction probabilities
        """
        if hasattr(self.model, 'predict_proba'):
            return self.model.predict_proba(X, **kwargs)
        else:
            raise NotImplementedError(
                f"Model {self.__class__.__name__} does not support predict_proba"
            )
    
    def save(self, path: str) -> bool:
        """
        Save model to disk.
        
        Args:
            path: Path to save the model
        
        Returns:
            True if successful, False otherwise
        """
        try:
            import pickle
            import os
            
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                pickle.dump({
                    'model': self.model,
                    'model_id': self.model_id,
                    'model_config': self.model_config,
                    'model_name': self.model_name,
                    'model_version': self.model_version,
                    'is_trained': self.is_trained
                }, f)
            
            self.logger.info(f"Model saved to {path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
            return False
    
    def load(self, path: str) -> bool:
        """
        Load model from disk.
        
        Args:
            path: Path to load the model from
        
        Returns:
            True if successful, False otherwise
        """
        try:
            import pickle
            
            with open(path, 'rb') as f:
                data = pickle.load(f)
            
            self.model = data['model']
            self.model_id = data.get('model_id', self.model_id)
            self.model_config = data.get('model_config', self.model_config)
            self.model_name = data.get('model_name', self.model_name)
            self.model_version = data.get('model_version', self.model_version)
            self.is_trained = data.get('is_trained', False)
            
            self.logger.info(f"Model loaded from {path}")
            return True
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get model information.
        
        Returns:
            Dictionary with model information
        """
        return {
            'model_id': self.model_id,
            'model_name': self.model_name,
            'model_version': self.model_version,
            'is_trained': self.is_trained,
            'model_config': self.model_config
        }

