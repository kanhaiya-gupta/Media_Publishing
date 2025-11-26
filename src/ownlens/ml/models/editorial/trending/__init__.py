"""
OwnLens - ML Module: Trending Topics Detection

Trending topics detection model, trainer, and predictor.
"""

from .model import TrendingTopicsModel
from .trainer import TrendingTopicsTrainer
from .predictor import TrendingTopicsPredictor

__all__ = [
    "TrendingTopicsModel",
    "TrendingTopicsTrainer",
    "TrendingTopicsPredictor",
]

