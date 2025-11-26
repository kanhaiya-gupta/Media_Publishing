"""
OwnLens - ML Module: Base Classes

Base classes and interfaces for ML models, trainers, and predictors.
"""

from .model import BaseMLModel
from .trainer import BaseTrainer
from .predictor import BasePredictor
from .evaluator import BaseEvaluator
from .feature_engineer import BaseFeatureEngineer

__all__ = [
    "BaseMLModel",
    "BaseTrainer",
    "BasePredictor",
    "BaseEvaluator",
    "BaseFeatureEngineer",
]

