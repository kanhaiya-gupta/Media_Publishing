"""
OwnLens - ML Module: Editorial Domain Models

Editorial domain ML models: performance prediction, trending detection, author performance,
headline optimization, and content recommendation.
"""

from .performance import ArticlePerformanceModel, ArticlePerformanceTrainer, ArticlePerformancePredictor
from .trending import TrendingTopicsModel, TrendingTopicsTrainer, TrendingTopicsPredictor
from .author import AuthorPerformanceModel, AuthorPerformanceTrainer, AuthorPerformancePredictor
from .headline import HeadlineOptimizationModel, HeadlineOptimizationTrainer, HeadlineOptimizationPredictor
from .recommendation import ContentRecommendationModel, ContentRecommendationTrainer, ContentRecommendationPredictor

__all__ = [
    "ArticlePerformanceModel",
    "ArticlePerformanceTrainer",
    "ArticlePerformancePredictor",
    "TrendingTopicsModel",
    "TrendingTopicsTrainer",
    "TrendingTopicsPredictor",
    "AuthorPerformanceModel",
    "AuthorPerformanceTrainer",
    "AuthorPerformancePredictor",
    "HeadlineOptimizationModel",
    "HeadlineOptimizationTrainer",
    "HeadlineOptimizationPredictor",
    "ContentRecommendationModel",
    "ContentRecommendationTrainer",
    "ContentRecommendationPredictor",
]

