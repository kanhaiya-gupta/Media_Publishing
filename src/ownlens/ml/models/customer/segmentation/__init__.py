"""
OwnLens - ML Module: User Segmentation

User segmentation model, trainer, and predictor.
"""

from .model import UserSegmentationModel
from .trainer import UserSegmentationTrainer
from .predictor import UserSegmentationPredictor

__all__ = [
    "UserSegmentationModel",
    "UserSegmentationTrainer",
    "UserSegmentationPredictor",
]

