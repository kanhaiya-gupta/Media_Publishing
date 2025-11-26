"""
OwnLens - ML Module: Base Trainer

Base class for model trainers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BaseTrainer(ABC):
    """
    Base class for model trainers.
    
    Handles the training workflow including:
    - Data loading
    - Feature engineering
    - Model training
    - Model evaluation
    - Model saving
    """
    
    def __init__(
        self,
        model: Any,
        feature_engineer: Optional[Any] = None,
        evaluator: Optional[Any] = None
    ):
        """
        Initialize the trainer.
        
        Args:
            model: ML model instance
            feature_engineer: Feature engineer instance
            evaluator: Model evaluator instance
        """
        self.model = model
        self.feature_engineer = feature_engineer
        self.evaluator = evaluator
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def load_data(self, **kwargs) -> Any:
        """
        Load training data.
        
        Args:
            **kwargs: Data loading parameters
        
        Returns:
            Training data
        """
        pass
    
    @abstractmethod
    def prepare_features(self, data: Any, **kwargs) -> Any:
        """
        Prepare features for training.
        
        Args:
            data: Raw data
            **kwargs: Feature engineering parameters
        
        Returns:
            Prepared features (X, y)
        """
        pass
    
    def train(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the complete training workflow.
        
        Args:
            **kwargs: Training parameters
        
        Returns:
            Dictionary with training results and metrics
        """
        self.logger.info("Starting training workflow...")
        
        # 1. Load data
        self.logger.info("Loading training data...")
        data = self.load_data(**kwargs)
        
        # 2. Prepare features
        self.logger.info("Preparing features...")
        X, y = self.prepare_features(data, **kwargs)
        
        # 3. Train model
        self.logger.info("Training model...")
        training_metrics = self.model.train(X, y, **kwargs)
        
        # 4. Evaluate model
        if self.evaluator:
            self.logger.info("Evaluating model...")
            evaluation_metrics = self.evaluator.evaluate(
                self.model,
                X,
                y,
                **kwargs
            )
            training_metrics.update(evaluation_metrics)
        
        self.logger.info("Training workflow completed")
        return training_metrics
    
    def save_model(self, path: str) -> bool:
        """
        Save the trained model.
        
        Args:
            path: Path to save the model
        
        Returns:
            True if successful, False otherwise
        """
        return self.model.save(path)

