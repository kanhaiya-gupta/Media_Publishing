"""
OwnLens - ML Module: Customer Domain Models

Customer domain ML models: churn, segmentation, conversion, recommendation.
"""

from .churn import ChurnModel, ChurnTrainer, ChurnPredictor, ChurnEvaluator
from .segmentation import UserSegmentationModel, UserSegmentationTrainer, UserSegmentationPredictor
from .conversion import ConversionModel, ConversionTrainer, ConversionPredictor
from .recommendation import UserRecommendationModel, UserRecommendationTrainer, UserRecommendationPredictor

__all__ = [
    "ChurnModel",
    "ChurnTrainer",
    "ChurnPredictor",
    "ChurnEvaluator",
    "UserSegmentationModel",
    "UserSegmentationTrainer",
    "UserSegmentationPredictor",
    "ConversionModel",
    "ConversionTrainer",
    "ConversionPredictor",
    "UserRecommendationModel",
    "UserRecommendationTrainer",
    "UserRecommendationPredictor",
]

